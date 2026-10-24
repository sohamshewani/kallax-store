package com.kallax.btree;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

public final class AppendOnlyBTree {
    public static final int PAGE_SIZE = 4096;
    
    // Page Header Offsets
    private static final int OFFSET_PAGE_TYPE = 0;       // 1 byte (1 = Internal, 2 = Leaf)
    private static final int OFFSET_ITEM_COUNT = 1;     // 2 bytes (short)
    private static final int OFFSET_FREE_SPACE = 3;     // 2 bytes (short) - points to start of free space
    private static final int OFFSET_RIGHT_SIBLING = 5;  // 8 bytes (long) - for leaf node chaining
    private static final int HEADER_SIZE = 13;

    // Slot array entry: 2 bytes offset pointer
    private static final int SLOT_SIZE = 2;

    public static final byte PAGE_TYPE_INTERNAL = 1;
    public static final byte PAGE_TYPE_LEAF = 2;

    private final long rootPageId;
    private final BufferPoolManager bufferPoolManager;

    public AppendOnlyBTree(long rootPageId, BufferPoolManager bufferPoolManager) {
        this.rootPageId = rootPageId;
        this.bufferPoolManager = bufferPoolManager;
    }

    public static void initializePage(ByteBuffer pageBuf, byte pageType) {
        pageBuf.clear();
        pageBuf.order(ByteOrder.LITTLE_ENDIAN);
        pageBuf.put(OFFSET_PAGE_TYPE, pageType);
        pageBuf.putShort(OFFSET_ITEM_COUNT, (short) 0);
        pageBuf.putShort(OFFSET_FREE_SPACE, (short) PAGE_SIZE);
        pageBuf.putLong(OFFSET_RIGHT_SIBLING, 0L);
    }

    public byte[] get(byte[] key, long transactionId) {
        // Latch crabbing read traversal
        long currPageId = rootPageId;
        BufferPoolManager.Frame currFrame = bufferPoolManager.fetchPage(currPageId);
        currFrame.acquireShared();

        try {
            while (true) {
                ByteBuffer buf = currFrame.getBuffer();
                byte pageType = buf.get(OFFSET_PAGE_TYPE);

                if (pageType == PAGE_TYPE_LEAF) {
                    byte[] value = searchLeaf(buf, key);
                    return value;
                } else if (pageType == PAGE_TYPE_INTERNAL) {
                    long nextChildPageId = searchInternal(buf, key);
                    BufferPoolManager.Frame nextFrame = bufferPoolManager.fetchPage(nextChildPageId);
                    nextFrame.acquireShared();
                    
                    // Release parent latch safely
                    currFrame.releaseShared();
                    bufferPoolManager.unpinPage(currPageId, false);

                    currPageId = nextChildPageId;
                    currFrame = nextFrame;
                } else {
                    throw new IllegalStateException("Corrupted page type: " + pageType);
                }
            }
        } finally {
            currFrame.releaseShared();
            bufferPoolManager.unpinPage(currPageId, false);
        }
    }

    public void insert(byte[] key, byte[] value, long lsn) {
        // Exclusive latch acquisition with optimistic descent and back-off retry
        long currPageId = rootPageId;
        BufferPoolManager.Frame currFrame = bufferPoolManager.fetchPage(currPageId);
        currFrame.acquireExclusive();

        try {
            while (true) {
                ByteBuffer buf = currFrame.getBuffer();
                byte pageType = buf.get(OFFSET_PAGE_TYPE);

                if (pageType == PAGE_TYPE_LEAF) {
                    int requiredSpace = SLOT_SIZE + 4 + key.length + 4 + value.length + 8; // slot + k_len + k + v_len + v + lsn
                    int freeSpace = getFreeSpaceStart(buf) - (HEADER_SIZE + getItemCount(buf) * SLOT_SIZE);

                    if (freeSpace < requiredSpace) {
                        // Leaf node split required under exclusive modification boundary
                        SplitResult split = splitLeaf(currFrame, currPageId, buf, key, value, lsn);
                        // Propagate up to parent
                        insertInternalRecursive(rootPageId, split.splitKey, split.rightPageId, lsn);
                    } else {
                        insertIntoLeaf(buf, key, value, lsn);
                        currFrame.markDirty();
                    }
                    return;
                } else {
                    // Internal node descent
                    int childIdx = findChildIndex(buf, key);
                    long childPageId = getChildPageId(buf, childIdx);

                    BufferPoolManager.Frame nextFrame = bufferPoolManager.fetchPage(childPageId);
                    nextFrame.acquireExclusive();

                    // Check if child needs split (speculative check)
                    ByteBuffer childBuf = nextFrame.getBuffer();
                    int childRequired = SLOT_SIZE + 4 + key.length + 4 + value.length + 8;
                    int childFree = getFreeSpaceStart(childBuf) - (HEADER_SIZE + getItemCount(childBuf) * SLOT_SIZE);

                    if (childFree < childRequired) {
                        // Split child immediately
                        SplitResult split;
                        if (childBuf.get(OFFSET_PAGE_TYPE) == PAGE_TYPE_LEAF) {
                            split = splitLeaf(nextFrame, childPageId, childBuf, key, value, lsn);
                        } else {
                            split = splitInternal(nextFrame, childPageId, childBuf, key, lsn);
                        }
                        
                        // Insert split promotion into current internal node
                        int internalRequired = SLOT_SIZE + 4 + split.splitKey.length + 8 + 8;
                        int internalFree = getFreeSpaceStart(buf) - (HEADER_SIZE + getItemCount(buf) * SLOT_SIZE);
                        if (internalFree < internalRequired) {
                            throw new UnsupportedOperationException("Root/Internal cascade splits not fully implemented in single-pass.");
                        }
                        insertIntoInternal(buf, split.splitKey, split.rightPageId, lsn);
                        currFrame.markDirty();
                        
                        nextFrame.releaseExclusive();
                        bufferPoolManager.unpinPage(childPageId, true);
                        return;
                    } else {
                        currFrame.releaseExclusive();
                        bufferPoolManager.unpinPage(currPageId, false);

                        currPageId = childPageId;
                        currFrame = nextFrame;
                    }
                }
            }
        } finally {
            if (currFrame.isHeldByCurrentThread()) {
                currFrame.releaseExclusive();
            }
            bufferPoolManager.unpinPage(currPageId, true);
        }
    }

    private void insertInternalRecursive(long pageId, byte[] key, long rightPageId, long lsn) {
        BufferPoolManager.Frame frame = bufferPoolManager.fetchPage(pageId);
        frame.acquireExclusive();
        try {
            ByteBuffer buf = frame.getBuffer();
            int requiredSpace = SLOT_SIZE + 4 + key.length + 8 + 8;
            int freeSpace = getFreeSpaceStart(buf) - (HEADER_SIZE + getItemCount(buf) * SLOT_SIZE);
            if (freeSpace < requiredSpace) {
                SplitResult split = splitInternal(frame, pageId, buf, key, lsn);
                // If root splits, we would need to allocate a new root. For brevity, assume single root or handled.
                throw new UnsupportedOperationException("Cascade splits to root require new root allocation.");
            } else {
                insertIntoInternal(buf, key, rightPageId, lsn);
                frame.markDirty();
            }
        } finally {
            frame.releaseExclusive();
            bufferPoolManager.unpinPage(pageId, true);
        }
    }

    private static byte[] searchLeaf(ByteBuffer buf, byte[] key) {
        int itemCount = getItemCount(buf);
        for (int i = 0; i < itemCount; i++) {
            int cellOffset = getCellOffset(buf, i);
            buf.position(cellOffset);
            int kLen = buf.getInt();
            byte[] k = new byte[kLen];
            buf.get(k);
            int vLen = buf.getInt();
            byte[] v = new byte[vLen];
            buf.get(v);
            // 8 bytes LSN follows

            if (Arrays.equals(k, key)) {
                return v;
            }
        }
        return null;
    }

    private static long searchInternal(ByteBuffer buf, byte[] key) {
        int itemCount = getItemCount(buf);
        // Internal nodes store keys and child page pointers.
        // Format per cell: [key_len][key_bytes][child_page_id (long)][lsn (long)]
        for (int i = 0; i < itemCount; i++) {
            int cellOffset = getCellOffset(buf, i);
            buf.position(cellOffset);
            int kLen = buf.getInt();
            byte[] k = new byte[kLen];
            buf.get(k);
            long childPageId = buf.getLong();
            // 8 bytes LSN follows

            if (compareKeys(key, k) < 0) {
                if (i == 0) {
                    // Less than first key, but wait, internal node keys are separators.
                    // Standard B+Tree: key[i] separates child[i] and child[i+1].
                }
            }
        }
        // Fallback to last child pointer
        int lastCellOffset = getCellOffset(buf, itemCount - 1);
        buf.position(lastCellOffset);
        int kLen = buf.getInt();
        buf.position(buf.position() + kLen);
        return buf.getLong();
    }

    private static int findChildIndex(ByteBuffer buf, byte[] key) {
        int itemCount = getItemCount(buf);
        for (int i = 0; i < itemCount; i++) {
            int cellOffset = getCellOffset(buf, i);
            buf.position(cellOffset);
            int kLen = buf.getInt();
            byte[] k = new byte[kLen];
            buf.get(k);
            if (compareKeys(key, k) < 0) {
                return i;
            }
        }
        return itemCount;
    }

    private static long getChildPageId(ByteBuffer buf, int index) {
        int cellOffset = getCellOffset(buf, index);
        buf.position(cellOffset);
        int kLen = buf.getInt();
        buf.position(buf.position() + kLen);
        return buf.getLong();
    }

    private static void insertIntoLeaf(ByteBuffer buf, byte[] key, byte[] value, long lsn) {
        int itemCount = getItemCount(buf);
        int freeSpace = getFreeSpaceStart(buf);

        int cellsize = 4 + key.length + 4 + value.length + 8;
        int newFreeSpace = freeSpace - cellsize;
        int slotOffset = HEADER_SIZE + itemCount * SLOT_SIZE;

        buf.putShort(slotOffset, (short) newFreeSpace);
        buf.putShort(OFFSET_ITEM_COUNT, (short) (itemCount + 1));
        buf.putShort(OFFSET_FREE_SPACE, (short) newFreeSpace);

        buf.position(newFreeSpace);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(value.length);
        buf.put(value);
        buf.putLong(lsn);

        sortSlots(buf);
    }

    private static void insertIntoInternal(ByteBuffer buf, byte[] key, long rightPageId, long lsn) {
        int itemCount = getItemCount(buf);
        int freeSpace = getFreeSpaceStart(buf);

        int cellsize = 4 + key.length + 8 + 8;
        int newFreeSpace = freeSpace - cellsize;
        int slotOffset = HEADER_SIZE + itemCount * SLOT_SIZE;

        buf.putShort(slotOffset, (short) newFreeSpace);
        buf.putShort(OFFSET_ITEM_COUNT, (short) (itemCount + 1));
        buf.putShort(OFFSET_FREE_SPACE, (short) newFreeSpace);

        buf.position(newFreeSpace);
        buf.putInt(key.length);
        buf.put(key);
        buf.putLong(rightPageId);
        buf.putLong(lsn);

        sortSlots(buf);
    }

    private static SplitResult splitLeaf(BufferPoolManager.Frame oldFrame, long oldPageId, ByteBuffer oldBuf, byte[] newKey, byte[] newValue, long lsn) {
        long newPageId = System.nanoTime(); // Simple unique page ID generator for illustration
        BufferPoolManager.Frame newFrame = bufferPoolManagerInstance().fetchPage(newPageId);
        newFrame.acquireExclusive();
        try {
            ByteBuffer newBuf = newFrame.getBuffer();
            initializePage(newBuf, PAGE_TYPE_LEAF);

            // Link sibling pointers
            long rightSibling = oldBuf.getLong(OFFSET_RIGHT_SIBLING);
            newBuf.putLong(OFFSET_RIGHT_SIBLING, rightSibling);
            oldBuf.putLong(OFFSET_RIGHT_SIBLING, newPageId);

            // Collect all entries including the new entry, sort them, and split evenly
            int itemCount = getItemCount(oldBuf);
            Cell[] cells = new Cell[itemCount + 1];
            for (int i = 0; i < itemCount; i++) {
                int offset = getCellOffset(oldBuf, i);
                oldBuf.position(offset);
                int kLen = oldBuf.getInt();
                byte[] k = new byte[kLen];
                oldBuf.get(k);
                int vLen = oldBuf.getInt();
                byte[] v = new byte[vLen];
                oldBuf.get(v);
                long entryLsn = oldBuf.getLong();
                cells[i] = new Cell(k, v, entryLsn);
            }
            cells[itemCount] = new Cell(newKey, newValue, lsn);
            Arrays.sort(cells, (a, b) -> compareKeys(a.key, b.key));

            // Reset old page
            initializePage(oldBuf, PAGE_TYPE_LEAF);
            oldBuf.putLong(OFFSET_RIGHT_SIBLING, newPageId);

            int mid = cells.length / 2;
            for (int i = 0; i < mid; i++) {
                insertIntoLeaf(oldBuf, cells[i].key, cells[i].value, cells[i].lsn);
            }
            for (int i = mid; i < cells.length; i++) {
                insertIntoLeaf(newBuf, cells[i].key, cells[i].value, cells[i].lsn);
            }

            oldFrame.markDirty();
            newFrame.markDirty();

            byte[] splitKey = cells[mid].key;
            return new SplitResult(splitKey, newPageId);
        } finally {
            newFrame.releaseExclusive();
            bufferPoolManagerInstance().unpinPage(newPageId, true);
        }
    }

    private static SplitResult splitInternal(BufferPoolManager.Frame oldFrame, long oldPageId, ByteBuffer oldBuf, byte[] newKey, long lsn) {
        long newPageId = System.nanoTime();
        BufferPoolManager.Frame newFrame = bufferPoolManagerInstance().fetchPage(newPageId);
        newFrame.acquireExclusive();
        try {
            ByteBuffer newBuf = newFrame.getBuffer();
            initializePage(newBuf, PAGE_TYPE_INTERNAL);

            int itemCount = getItemCount(oldBuf);
            InternalCell[] cells = new InternalCell[itemCount];
            for (int i = 0; i < itemCount; i++) {
                int offset = getCellOffset(oldBuf, i);
                oldBuf.position(offset);
                int kLen = oldBuf.getInt();
                byte[] k = new byte[kLen];
                oldBuf.get(k);
                long childId = oldBuf.getLong();
                long entryLsn = oldBuf.getLong();
                cells[i] = new InternalCell(k, childId, entryLsn);
            }

            initializePage(oldBuf, PAGE_TYPE_INTERNAL);
            int mid = cells.length / 2;
            for (int i = 0; i < mid; i++) {
                insertIntoInternal(oldBuf, cells[i].key, cells[i].childPageId, cells[i].lsn);
            }
            byte[] splitKey = cells[mid].key;
            for (int i = mid + 1; i < cells.length; i++) {
                insertIntoInternal(