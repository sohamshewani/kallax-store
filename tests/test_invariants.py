package com.kallax.store;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * KallaxStore is an embedded, concurrent, durable key-value store.
 * It provides transactional writes, append-only log-structured storage, crash recovery,
 * snapshot compaction, and strict invariant enforcement under high thread contention.
 */
class KallaxStore implements Closeable {

    private final Path dbDir;
    private final Path dataFile;
    private final Path indexFile;
    private final Path lockFile;
    private FileChannel fileChannel;
    private FileLock fileLock;
    
    private final ConcurrentSkipListMap<String, IndexEntry> memoryIndex = new ConcurrentSkipListMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private long writeOffset = 0;
    private volatile boolean isClosed = false;

    private static final byte OP_PUT = 0x01;
    private static final byte OP_DEL = 0x02;

    public static class IndexEntry {
        final long offset;
        final int length;
        final long timestamp;
        final boolean isDeleted;

        public IndexEntry(long offset, int length, long timestamp, boolean isDeleted) {
            this.offset = offset;
            this.length = length;
            this.timestamp = timestamp;
            this.isDeleted = isDeleted;
        }
    }

    public KallaxStore(Path dbDir) throws IOException {
        this.dbDir = dbDir;
        Files.createDirectories(dbDir);
        this.dataFile = dbDir.resolve("kallax.data");
        this.indexFile = dbDir.resolve("kallax.idx");
        this.lockFile = dbDir.resolve("kallax.lock");

        acquireExclusiveLock();
        openDataFile();
        recover();
    }

    private void acquireExclusiveLock() throws IOException {
        RandomAccessFile lockRaf = new RandomAccessFile(lockFile.toFile(), "rw");
        this.fileLock = lockRaf.getChannel().tryLock();
        if (this.fileLock == null) {
            lockRaf.close();
            throw new IOException("Database directory is already locked by another process: " + dbDir);
        }
    }

    private void openDataFile() throws IOException {
        RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw");
        this.fileChannel = raf.getChannel();
        this.writeOffset = fileChannel.size();
    }

    private void recover() throws IOException {
        if (!Files.exists(dataFile) || Files.size(dataFile) == 0) {
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "r");
             FileChannel channel = raf.getChannel()) {
            
            ByteBuffer headerBuffer = ByteBuffer.allocate(21); // keyLen(4) + valLen(4) + ts(8) + op(1) + dataLen(4)
            long position = 0;
            
            while (position < channel.size()) {
                channel.position(position);
                headerBuffer.clear();
                int read = channel.read(headerBuffer);
                if (read < 21) {
                    // Truncate trailing partial writes (crash recovery)
                    break;
                }
                headerBuffer.flip();
                int keyLen = headerBuffer.getInt();
                int valLen = headerBuffer.getInt();
                long timestamp = headerBuffer.getLong();
                byte op = headerBuffer.get();
                int totalPayloadLen = headerBuffer.getInt();

                if (keyLen < 0 || keyLen > 1024 * 1024 || valLen < 0 || valLen > 10 * 1024 * 1024) {
                    // Corrupted record, stop recovery here
                    break;
                }

                ByteBuffer recordBuffer = ByteBuffer.allocate(keyLen + valLen);
                int recRead = channel.read(recordBuffer, position + 21);
                if (recRead < keyLen + valLen) {
                    break;
                }
                recordBuffer.flip();

                byte[] keyBytes = new byte[keyLen];
                recordBuffer.get(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int recordDiskSize = 21 + keyLen + valLen;

                if (op == OP_PUT) {
                    memoryIndex.put(key, new IndexEntry(position, recordDiskSize, timestamp, false));
                } else if (op == OP_DEL) {
                    memoryIndex.put(key, new IndexEntry(position, recordDiskSize, timestamp, true));
                }

                position += recordDiskSize;
            }
            this.writeOffset = position;
            // Truncate any unrecovered garbage at the tail
            fileChannel.truncate(writeOffset);
        }
    }

    public void put(String key, byte[] value) throws IOException {
        validateKeyAndValue(key, value);
        writeLock.lock();
        try {
            ensureOpen();
            long timestamp = System.nanoTime();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            int keyLen = keyBytes.length;
            int valLen = value.length;
            int totalPayloadLen = keyLen + valLen;
            int recordSize = 21 + totalPayloadLen;

            ByteBuffer buf = ByteBuffer.allocate(recordSize);
            buf.putInt(keyLen);
            buf.putInt(valLen);
            buf.putLong(timestamp);
            buf.put(OP_PUT);
            buf.putInt(totalPayloadLen);
            buf.put(keyBytes);
            buf.put(value);
            buf.flip();

            long currentOffset = writeOffset;
            fileChannel.write(buf, currentOffset);
            fileChannel.force(false); // Durability guarantee

            memoryIndex.put(key, new IndexEntry(currentOffset, recordSize, timestamp, false));
            writeOffset += recordSize;
        } finally {
            writeLock.unlock();
        }
    }

    public byte[] get(String key) throws IOException {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        readLock.lock();
        try {
            ensureOpen();
            IndexEntry entry = memoryIndex.get(key);
            if (entry == null || entry.isDeleted) {
                return null;
            }

            ByteBuffer buf = ByteBuffer.allocate(entry.length);
            fileChannel.read(buf, entry.offset);
            buf.flip();

            int keyLen = buf.getInt();
            int valLen = buf.getInt();
            buf.getLong(); // timestamp
            buf.get();     // op
            buf.getInt();  // payload len

            // Skip key bytes
            buf.position(buf.position() + keyLen);
            byte[] value = new byte[valLen];
            buf.get(value);
            return value;
        } finally {
            readLock.unlock();
        }
    }

    public boolean delete(String key) throws IOException {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        writeLock.lock();
        try {
            ensureOpen();
            IndexEntry existing = memoryIndex.get(key);
            if (existing == null || existing.isDeleted) {
                return false;
            }

            long timestamp = System.nanoTime();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            int keyLen = keyBytes.length;
            int recordSize = 21 + keyLen;

            ByteBuffer buf = ByteBuffer.allocate(recordSize);
            buf.putInt(keyLen);
            buf.putInt(0); // valLen = 0
            buf.putLong(timestamp);
            buf.put(OP_DEL);
            buf.putInt(keyLen);
            buf.put(keyBytes);
            buf.flip();

            long currentOffset = writeOffset;
            fileChannel.write(buf, currentOffset);
            fileChannel.force(false);

            memoryIndex.put(key, new IndexEntry(currentOffset, recordSize, timestamp, true));
            writeOffset += recordSize;
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public void compact() throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            Path tempFile = dbDir.resolve("kallax.data.tmp");
            try (FileChannel tempChannel = FileChannel.open(tempFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                
                long newOffset = 0;
                Map<String, IndexEntry> newIndex = new HashMap<>();

                for (Map.Entry<String, IndexEntry> mapEntry : memoryIndex.entrySet()) {
                    String key = mapEntry.getKey();
                    IndexEntry entry = mapEntry.getValue();

                    if (entry.isDeleted) {
                        continue;
                    }

                    // Read original record
                    ByteBuffer readBuf = ByteBuffer.allocate(entry.length);
                    fileChannel.read(readBuf, entry.offset);
                    readBuf.flip();

                    // Write to temp file
                    tempChannel.write(readBuf, newOffset);
                    
                    int keyLen = readBuf.getInt(0);
                    int valLen = readBuf.getInt(4);
                    int totalDiskSize = 21 + keyLen + valLen;

                    newIndex.put(key, new IndexEntry(newOffset, totalDiskSize, entry.timestamp, false));
                    newOffset += totalDiskSize;
                }
                tempChannel.force(true);

                fileChannel.close();
                Files.move(tempFile, dataFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                
                openDataFile();
                memoryIndex.clear();
                memoryIndex.putAll(newIndex);
                writeOffset = newOffset;
            } finally {
                Files.deleteIfExists(tempFile);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public int size() {
        readLock.lock();
        try {
            int count = 0;
            for (IndexEntry entry : memoryIndex.values()) {
                if (!entry.isDeleted) count++;
            }
            return count;
        } finally {
            readLock.unlock();
        }
    }

    public Set<String> keySet() {
        readLock.lock();
        try {
            Set<String> keys = new HashSet<>();
            for (Map.Entry<String, IndexEntry> entry : memoryIndex.entrySet()) {
                if (!entry.getValue().isDeleted) {
                    keys.add(entry.getKey());
                }
            }
            return keys;
        } finally {
            readLock.unlock();
        }
    }

    private void validateKeyAndValue(String key, byte[] value) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        if (key.getBytes(StandardCharsets.UTF_8).length > 1024) {
            throw new IllegalArgumentException("Key exceeds maximum length of 1024 bytes");
        }
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Store is closed");
        }
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (isClosed) return;
            isClosed = true;
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (fileLock != null) {
                fileLock.release();
            }
        } finally {
            writeLock.unlock();
        }
    }
}

class KallaxStoreTest {

    @TempDir
    Path