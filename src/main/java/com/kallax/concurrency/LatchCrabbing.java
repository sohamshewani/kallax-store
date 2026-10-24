package com.kallax.concurrency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

public final class LatchCrabbing {

    private final StampedLock lock = new StampedLock();
    private final VarHandle stateHandle;
    @SuppressWarnings("unused")
    private volatile int state = 0;

    public enum LatchMode {
        SHARED,
        EXCLUSIVE,
        OPTIMISTIC
    }

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            stateHandle = l.findVarHandle(LatchCrabbing.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public LatchCrabbing() {
    }

    public long acquire(LatchMode mode) {
        switch (mode) {
            case SHARED:
                return lock.readLock();
            case EXCLUSIVE:
                return lock.writeLock();
            case OPTIMISTIC:
                return lock.tryOptimisticRead();
            default:
                throw new IllegalArgumentException("Unsupported latch mode: " + mode);
        }
    }

    public void release(LatchMode mode, long stamp) {
        switch (mode) {
            case SHARED:
                lock.unlockRead(stamp);
                break;
            case EXCLUSIVE:
                lock.unlockWrite(stamp);
                break;
            case OPTIMISTIC:
                // Optimistic locks do not need explicit release unless validation fails
                break;
            default:
                throw new IllegalArgumentException("Unsupported latch mode: " + mode);
        }
    }

    public boolean validateOptimistic(long stamp) {
        return lock.validate(stamp);
    }

    public long upgradeToExclusive(long sharedStamp) {
        long writeStamp = lock.tryConvertToWriteLock(sharedStamp);
        if (writeStamp == 0L) {
            lock.unlockRead(sharedStamp);
            writeStamp = lock.writeLock();
        }
        return writeStamp;
    }

    public long downgradeToShared(long writeStamp) {
        long readStamp = lock.tryConvertToReadLock(writeStamp);
        if (readStamp == 0L) {
            lock.unlockWrite(writeStamp);
            readStamp = lock.readLock();
        }
        return readStamp;
    }

    public boolean tryAcquire(LatchMode mode, long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        switch (mode) {
            case SHARED:
                long sStamp = lock.tryReadLock(nanos, TimeUnit.NANOSECONDS);
                return sStamp != 0L;
            case EXCLUSIVE:
                long eStamp = lock.tryWriteLock(nanos, TimeUnit.NANOSECONDS);
                return eStamp != 0L;
            case OPTIMISTIC:
                return lock.tryOptimisticRead() != 0L;
            default:
                throw new IllegalArgumentException("Unsupported latch mode: " + mode);
        }
    }
}