package com.kallax.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

/**
 * Production-grade Write-Ahead Log (WAL) implementation for the kallax-store storage engine.
 * Handles append-only log records with CRC32 checksum verification, segment management,
 * LSN generation, and forced synchronization to enforce the Write-Ahead rule.
 */
public final class WriteAheadLog implements AutoCloseable {

    public static final int LOG_PAGE_SIZE = 4096;
    public static final long INVALID_LSN = -1L;
    
    // Record Type Identifiers
    public static final byte RECORD_TYPE_BEGIN = 0x01;
    public static final byte RECORD_TYPE_COMMIT = 0x02;
    public static final byte RECORD_TYPE_ABORT = 0x03;
    public static final byte RECORD_TYPE_UPDATE = 0x04;
    public static final byte RECORD_TYPE_CLR = 0x05;
    public static final byte RECORD_TYPE_CHECKPOINT = 0x06;

    private static final int HEADER_SIZE = 8; // CRC32 (4 bytes) + Payload Length (4 bytes)
    private static final int SEGMENT_FILE_SIZE = 64 * 1024 * 1024; // 64 MB per WAL segment

    private final Path logDir;
    private final AtomicLong lsnSequence;
    private final ReentrantReadWriteLock logLock;

    private long currentSegmentId;
    private FileChannel currentChannel;
    private ByteBuffer activeBuffer;
    
    // Tracks flushed LSN to enforce WAL-to-disk atomicity invariants
    private final AtomicLong flushedLsn;
    
    // Active file handle mapping for recovery and log scanning
    private final ConcurrentMap<Long, File> segmentFiles;

    public WriteAheadLog(Path logDir) throws IOException {
        this.logDir = logDir;
        this.lsnSequence = new AtomicLong(0L);
        this.logLock = new ReentrantReadWriteLock();
        this.flushedLsn = new AtomicLong(INVALID_LSN);
        this.segmentFiles = new ConcurrentHashMap<>();

        if (!java.nio.file.Files.exists(logDir)) {
            java.nio.file.Files.createDirectories(logDir);
        }

        initializeLatestSegment();
    }

    private void initializeLatestSegment() throws IOException {
        // Scan directory for existing segments to determine current segment ID and maximum LSN
        long maxSegment = 0L;
        long maxLsnFound = 0L;

        File dir = logDir.toFile();
        File[] files = dir.listFiles((d, name) -> name.endsWith(".wal"));
        if (files != null) {
            for (File f : files) {
                String name = f.getName();
                String idStr = name.substring(0, name.indexOf('.'));
                try {
                    long segId = Long.parseLong(idStr);
                    segmentFiles.put(segId, f);
                    if (segId > maxSegment) {
                        maxSegment = segId;
                    }
                } catch (NumberFormatException ignored) {
                }
            }
        }

        if (segmentFiles.isEmpty()) {
            currentSegmentId = 0L;
            createNewSegment(currentSegmentId);
        } else {
            currentSegmentId = maxSegment;
            File segFile = segmentFiles.get(currentSegmentId);
            currentChannel = FileChannel.open(segFile.toPath(), 
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            long size = currentChannel.size();
            currentChannel.position(size);
            
            // Scan segment to discover the true max LSN
            maxLsnFound = scanSegmentForMaxLsn(segFile, currentSegmentId);
            lsnSequence.set(maxLsnFound);
        }

        activeBuffer = ByteBuffer.allocateDirect(LOG_PAGE_SIZE);
    }

    private void createNewSegment(long segmentId) throws IOException {
        if (currentChannel != null) {
            currentChannel.force(true);
            currentChannel.close();
        }

        File segFile = logDir.resolve(String.format("%016d.wal", segmentId)).toFile();
        segmentFiles.put(segmentId, segFile);
        
        currentChannel = FileChannel.open(segFile.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        currentChannel.truncate(0L);
    }

    private long scanSegmentForMaxLsn(File file, long segmentId) throws IOException {
        long maxLsn = segmentId * 1_000_000L; // Base LSN offset approximation if empty
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {
            ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
            while (channel.position() < channel.size()) {
                headerBuf.clear();
                int read = channel.read(headerBuf);
                if (read < HEADER_SIZE) {
                    break;
                }
                headerBuf.flip();
                int crc = headerBuf.getInt();
                int length = headerBuf.getInt();

                ByteBuffer payloadBuf = ByteBuffer.allocate(length);
                int payloadRead = channel.read(payloadBuf);
                if (payloadRead < length) {
                    break;
                }
                payloadBuf.flip();
                
                // Read LSN from payload (Assume LSN is the first 8 bytes of payload)
                if (length >= 8) {
                    long lsn = payloadBuf.getLong();
                    if (lsn > maxLsn) {
                        maxLsn = lsn;
                    }
                }
            }
        }
        return maxLsn;
    }

    /**
     * Appends a log record to the WAL and returns its assigned LSN.
     * Thread-safe via exclusive log locking.
     */
    public long append(byte recordType, long txId, long pageId, byte[] payload) throws IOException {
        logLock.writeLock().lock();
        try {
            long lsn = lsnSequence.incrementAndGet();
            
            // Calculate total record length:
            // HEADER: CRC32 (4) + Length (4)
            // BODY: LSN (8) + TxId (8) + RecordType (1) + PageId (8) + PayloadLength (4) + Payload (N)
            int bodyLength = 8 + 8 + 1 + 8 + 4 + (payload != null ? payload.length : 0);
            int totalPacketSize = HEADER_SIZE + bodyLength;

            // Check if current segment exceeds size limit
            if (currentChannel.position() + totalPacketSize > SEGMENT_FILE_SIZE) {
                currentSegmentId++;
                createNewSegment(currentSegmentId);
            }

            ByteBuffer recordBuffer = ByteBuffer.allocate(totalPacketSize);
            
            // Populate body
            recordBuffer.position(HEADER_SIZE);
            recordBuffer.putLong(lsn);
            recordBuffer.putLong(txId);
            recordBuffer.put(recordType);
            recordBuffer.putLong(pageId);
            if (payload != null) {
                recordBuffer.putInt(payload.length);
                recordBuffer.put(payload);
            } else {
                recordBuffer.putInt(0);
            }
            
            int bodyLimit = recordBuffer.position();
            
            // Compute CRC32 over the body
            CRC32 crc32 = new CRC32();
            crc32.update(recordBuffer.array(), HEADER_SIZE, bodyLimit - HEADER_SIZE);
            int checksum = (int) crc32.getValue();

            // Write Header
            recordBuffer.position(0);
            recordBuffer.putInt(checksum);
            recordBuffer.putInt(bodyLimit - HEADER_SIZE);
            
            recordBuffer.position(0);
            recordBuffer.limit(bodyLimit);

            // Write to file channel
            currentChannel.write(recordBuffer);

            return lsn;
        } finally {
            logLock.writeLock().unlock();
        }
    }

    /**
     * Forces all log records up to the current sequence to be flushed to non-volatile storage.
     * Enforces the WAL-to-Disk atomicity invariant.
     */
    public void flush() throws IOException {
        long currentMaxLsn = lsnSequence.get();
        if (currentMaxLsn <= flushedLsn.get()) {
            return;
        }

        logLock.writeLock().lock();
        try {
            if (currentChannel != null && currentChannel.isOpen()) {
                currentChannel.force(true);
                flushedLsn.set(currentMaxLsn);
            }
        } finally {
            logLock.writeLock().unlock();
        }
    }

    /**
     * Forces flush up to a specific LSN to satisfy dependent dirty page write-requests.
     */
    public void flush(long targetLsn) throws IOException {
        if (targetLsn <= flushedLsn.get()) {
            return;
        }
        logLock.writeLock().lock();
        try {
            if (currentChannel != null && currentChannel.isOpen()) {
                currentChannel.force(true);
                flushedLsn.set(lsnSequence.get());
            }
        } finally {
            logLock.writeLock().unlock();
        }
    }

    public long getFlushedLsn() {
        return flushedLsn.get();
    }

    public long getCurrentLsn() {
        return lsnSequence.get();
    }

    /**
     * Log record representation for iteration, analysis, and ARIES recovery passes.
     */
    public static final class LogRecord {
        private final long lsn;
        private final long txId;
        private final byte recordType;
        private final long pageId;
        private final byte[] payload;

        public LogRecord(long lsn, long txId, byte recordType, long pageId, byte[] payload) {
            this.lsn = lsn;
            this.txId = txId;
            this.recordType = recordType;
            this.pageId = pageId;
            this.payload = payload;
        }

        public long getLsn() { return lsn; }
        public long getTxId() { return txId; }
        public byte getRecordType() { return recordType; }
        public long getPageId() { return pageId; }
        public byte[] getPayload() { return payload; }
    }

    /**
     * Returns an iterator/scanner over all stored WAL records for recovery analysis and execution passes.
     */
    public List<LogRecord> readAllRecords() throws IOException {
        List<LogRecord> records = new ArrayList<>();
        List<Long> sortedSegments = new ArrayList<>(segmentFiles.keySet());
        sortedSegments.sort(Long::compareTo);

        for (Long segId : sortedSegments) {
            File file = segmentFiles.get(segId);
            try (RandomAccessFile raf = new RandomAccessFile(file, "r");
                 FileChannel channel = raf.getChannel()) {
                ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
                while (channel.position() < channel.size()) {
                    headerBuf.clear();
                    int read = channel.read(headerBuf);
                    if (read < HEADER_SIZE) {
                        break;
                    }
                    headerBuf.flip();
                    int expectedCrc = headerBuf.getInt();
                    int length = headerBuf.getInt();

                    ByteBuffer payloadBuf = ByteBuffer.allocate(length);
                    int payloadRead = channel.read(payloadBuf);
                    if (payloadRead < length) {
                        break;
                    }
                    payloadBuf.flip();

                    // Verify CRC32
                    CRC32 crc32 = new CRC32();
                    crc32.update(payloadBuf.array(), payloadBuf.arrayOffset(), length);
                    int actualCrc = (int) crc32.getValue();

                    if (actualCrc != expectedCrc) {
                        throw new IOException(String.format(
                            "WAL corruption detected! CRC32 mismatch on segment %d. Expected: 0x%08X, Actual: 0x%08X",
                            segId, expectedCrc, actualCrc));
                    }

                    long lsn = payloadBuf.getLong();
                    long txId = payloadBuf.getLong();
                    byte recordType = payloadBuf.get();
                    long pageId = payloadBuf.getLong();
                    int payloadLen = payloadBuf.getInt();
                    
                    byte[] payload = null;
                    if (payloadLen > 0) {
                        payload = new byte[payloadLen];
                        payloadBuf.get(payload);
                    }

                    records.add(new LogRecord(lsn, txId, recordType, pageId, payload));
                }
            }
        }
        return records;
    }

    @Override
    public void close() throws IOException {
        logLock.writeLock().lock();
        try {
            if (currentChannel != null && currentChannel.isOpen()) {
                currentChannel.force(true);
                currentChannel.close();
            }
            if (activeBuffer != null) {
                // Direct ByteBuffer cleanup if required, managed via GC or explicit unmapping hooks if available.
                activeBuffer = null;
            }
        } finally {
            logLock.writeLock().unlock();
        }
    }
}