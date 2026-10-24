package com.kallax.memory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BufferPoolManager {
    public static final int PAGE_SIZE = 4096;

    private final int poolSize;
    private final Path dbFilePath;
    private final Frame[] frames;
    private final ConcurrentHashMap<Long, Integer> pageTable;
    private final AtomicInteger clockHand;
    private final FileChannel fileChannel;
    private final ReentrantReadWriteLock bpmLock;

    public static class Frame {
        private final int frameId;
        private final ByteBuffer memory;
        private final AtomicLong pageId;
        private final AtomicInteger pinCount;
        private final AtomicInteger isDirty;
        private final AtomicInteger refBit;
        private final AtomicInteger secondChanceBit;
        private final ReentrantReadWriteLock rwLock;

        public Frame(int frameId) {
            this.frameId = frameId;
            this.memory = ByteBuffer.allocateDirect(PAGE_SIZE);
            this.pageId = new AtomicLong(-1L);
            this.pinCount = new AtomicInteger(0);
            this.isDirty = new AtomicInteger(0);
            this.refBit = new AtomicInteger(0);
            this.secondChanceBit = new AtomicInteger(0);
            this.rwLock = new ReentrantReadWriteLock();
        }

        public ByteBuffer getMemory() {
            return memory;
        }

        public long getPageId() {
            return pageId.get();
        }

        public void setPageId(long id) {
            pageId.set(id);
        }

        public int getPinCount() {
            return pinCount.get();
        }

        public int incrementPinCount() {
            return pinCount.incrementAndGet();
        }

        public int decrementPinCount() {
            return pinCount.decrementAndGet();
        }

        public boolean isDirty() {
            return isDirty.get() == 1;
        }

        public void setDirty(boolean dirty) {
            isDirty.set(dirty ? 1 : 0);
        }

        public int getRefBit() {
            return refBit.get();
        }

        public void setRefBit(int val) {
            refBit.set(val);
        }

        public int getSecondChanceBit() {
            return secondChanceBit.get();
        }

        public void setSecondChanceBit(int val) {
            secondChanceBit.set(val);
        }

        public ReentrantReadWriteLock getRwLock() {
            return rwLock;
        }

        public void clear() {
            rwLock.writeLock().lock();
            try {
                pageId.set(-1L);
                pinCount.set(0);
                isDirty.set(0);
                refBit.set(0);
                secondChanceBit.set(0);
                memory.clear();
                memory.put(new byte[PAGE_SIZE]);
                memory.clear();
            } finally {
                rwLock.writeLock().unlock();
            }
        }
    }

    public BufferPoolManager(int poolSize, Path dbFilePath) throws IOException {
        this.poolSize = poolSize;
        this.dbFilePath = dbFilePath;
        this.frames = new Frame[poolSize];
        for (int i = 0; i < poolSize; i++) {
            this.frames[i] = new Frame(i);
        }
        this.pageTable = new ConcurrentHashMap<>();
        this.clockHand = new AtomicInteger(0);
        this.bpmLock = new ReentrantReadWriteLock();

        if (!Files.exists(dbFilePath)) {
            Files.createDirectories(dbFilePath.getParent());
        }
        this.fileChannel = FileChannel.open(dbFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    public Frame fetchPage(long pageId) throws IOException {
        bpmLock.writeLock().lock();
        try {
            Integer frameIndexObj = pageTable.get(pageId);
            if (frameIndexObj != null) {
                int frameIndex = frameIndexObj;
                Frame frame = frames[frameIndex];
                frame.getRwLock().writeLock().lock();
                try {
                    frame.incrementPinCount();
                    frame.setRefBit(1);
                    frame.setSecondChanceBit(1);
                    return frame;
                } finally {
                    frame.getRwLock().writeLock().unlock();
                }
            }

            int victimFrameIndex = evict();
            Frame victimFrame = frames[victimFrameIndex];

            victimFrame.getRwLock().writeLock().lock();
            try {
                long oldPageId = victimFrame.getPageId();
                if (oldPageId != -1L) {
                    if (victimFrame.isDirty()) {
                        flushFrameToDisk(victimFrame);
                    }
                    pageTable.remove(oldPageId);
                }

                victimFrame.clear();
                victimFrame.setPageId(pageId);
                victimFrame.incrementPinCount();
                victimFrame.setRefBit(1);
                victimFrame.setSecondChanceBit(1);

                ByteBuffer buf = victimFrame.getMemory();
                buf.clear();
                long position = pageId * PAGE_SIZE;
                if (fileChannel.size() >= position + PAGE_SIZE) {
                    fileChannel.read(buf, position);
                } else {
                    buf.put(new byte[PAGE_SIZE]);
                }
                buf.clear();

                pageTable.put(pageId, victimFrameIndex);
                return victimFrame;
            } finally {
                victimFrame.getRwLock().writeLock().unlock();
            }
        } finally {
            bpmLock.writeLock().unlock();
        }
    }

    public boolean unpinPage(long pageId, boolean isDirty) {
        bpmLock.writeLock().lock();
        try {
            Integer frameIndexObj = pageTable.get(pageId);
            if (frameIndexObj == null) {
                return false;
            }
            Frame frame = frames[frameIndexObj];
            frame.getRwLock().writeLock().lock();
            try {
                if (frame.getPinCount() <= 0) {
                    return false;
                }
                if (isDirty) {
                    frame.setDirty(true);
                }
                int count = frame.decrementPinCount();
                if (count == 0) {
                    frame.setSecondChanceBit(1);
                }
                return true;
            } finally {
                frame.getRwLock().writeLock().unlock();
            }
        } finally {
            bpmLock.writeLock().unlock();
        }
    }

    public boolean flushPage(long pageId) throws IOException {
        bpmLock.writeLock().lock();
        try {
            Integer frameIndexObj = pageTable.get(pageId);
            if (frameIndexObj == null) {
                return false;
            }
            Frame frame = frames[frameIndexObj];
            frame.getRwLock().writeLock().lock();
            try {
                if (!frame.isDirty()) {
                    return true;
                }
                flushFrameToDisk(frame);
                return true;
            } finally {
                frame.getRwLock().writeLock().unlock();
            }
        } finally {
            bpmLock.writeLock().unlock();
        }
    }

    public Frame newPage(long pageId) throws IOException {
        bpmLock.writeLock().lock();
        try {
            if (pageTable.containsKey(pageId)) {
                return null;
            }
            int victimFrameIndex = evict();
            Frame victimFrame = frames[victimFrameIndex];

            victimFrame.getRwLock().writeLock().lock();
            try {
                long oldPageId = victimFrame.getPageId();
                if (oldPageId != -1L) {
                    if (victimFrame.isDirty()) {
                        flushFrameToDisk(victimFrame);
                    }
                    pageTable.remove(oldPageId);
                }

                victimFrame.clear();
                victimFrame.setPageId(pageId);
                victimFrame.incrementPinCount();
                victimFrame.setRefBit(1);
                victimFrame.setSecondChanceBit(1);
                victimFrame.setDirty(true);

                pageTable.put(pageId, victimFrameIndex);
                return victimFrame;
            } finally {
                victimFrame.getRwLock().writeLock().unlock();
            }
        } finally {
            bpmLock.writeLock().unlock();
        }
    }

    public boolean deletePage(long pageId) throws IOException {
        bpmLock.writeLock().lock();
        try {
            Integer frameIndexObj = pageTable.get(pageId);
            if (frameIndexObj == null) {
                return true;
            }
            Frame frame = frames[frameIndexObj];
            frame.getRwLock().writeLock().lock();
            try {
                if (frame.getPinCount() > 0) {
                    return false;
                }
                if (frame.isDirty()) {
                    flushFrameToDisk(frame);
                }
                pageTable.remove(pageId);
                frame.clear();
                return true;
            } finally {
                frame.getRwLock().writeLock().unlock();
            }
        } finally {
            bpmLock.writeLock().unlock();
        }
    }

    public void flushAllPages() throws IOException {
        bpmLock.writeLock().lock();
        try {
            for (int i = 0; i < poolSize; i++) {
                Frame frame = frames[i];
                frame.getRwLock().writeLock().lock();
                try {
                    if (frame.getPageId() != -1L && frame.isDirty()) {
                        flushFrameToDisk(frame);
                    }
                } finally {
                    frame.getRwLock().writeLock().unlock();
                }
            }
        } finally {
            bpmLock.writeLock().unlock();
        }
    }

    private int evict() {
        int hand = clockHand.get();
        int scanned = 0;
        while (scanned < poolSize * 2) {
            Frame frame = frames[hand];
            if (frame.getPageId() == -1L) {
                clockHand.set((hand + 1) % poolSize);
                return hand;
            }

            if (frame.getPinCount() == 0) {
                if (frame.getRefBit() == 1) {
                    frame.setRefBit(0);
                } else if (frame.getSecondChanceBit() == 1) {
                    frame.setSecondChanceBit(0);
                } else {
                    int victim = hand;
                    clockHand.set((hand + 1) % poolSize);
                    return victim;
                }
            }

            hand = (hand + 1) % poolSize;
            scanned++;
            clockHand.set(hand);
        }

        hand = clockHand.get();
        scanned = 0;
        while (scanned < poolSize) {
            Frame frame = frames[hand];
            if (frame.getPinCount() == 0) {
                int victim = hand;
                clockHand.set((hand + 1) % poolSize);
                return victim;
            }
            hand = (hand + 1) % poolSize;
            scanned++;
            clockHand.set(hand);
        }

        throw new IllegalStateException("All frames in BufferPool are currently pinned!");
    }

    private void flushFrameToDisk(Frame frame) throws IOException {
        ByteBuffer buf = frame.getMemory();
        buf.clear();
        long position = frame.getPageId() * PAGE_SIZE;
        fileChannel.write(buf, position);
        fileChannel.force(false);
        buf.clear();
        frame.setDirty(false);
    }

    public void close() throws IOException {
        flushAllPages();
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
    }
}