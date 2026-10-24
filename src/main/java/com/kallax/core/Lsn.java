package com.kallax.core;

import java.io.Serializable;
import java.util.Objects;

/**
 * Log Sequence Number (LSN) implementation for Kallax-Store.
 * Represents a monotonically increasing 64-bit integer tracking WAL byte offsets
 * and transaction progression. Conforms to ARIES recovery invariants.
 */
public final class Lsn implements Comparable<Lsn>, Serializable {
    private static final long serialVersionUID = 1L;

    public static final long INVALID_VAL = -1L;
    public static final Lsn INVALID_LSN = new Lsn(INVALID_VAL);
    public static final Lsn ZERO_LSN = new Lsn(0L);

    private final long value;

    public Lsn(long value) {
        if (value < INVALID_VAL) {
            throw new IllegalArgumentException("LSN value cannot be less than " + INVALID_VAL);
        }
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public boolean isValid() {
        return this.value != INVALID_VAL;
    }

    public Lsn next() {
        if (!isValid()) {
            throw new IllegalStateException("Cannot increment an invalid LSN.");
        }
        return new Lsn(this.value + 1L);
    }

    public Lsn add(long delta) {
        if (!isValid()) {
            throw new IllegalStateException("Cannot perform arithmetic on an invalid LSN.");
        }
        if (delta < 0L) {
            throw new IllegalArgumentException("Delta must be non-negative.");
        }
        return new Lsn(this.value + delta);
    }

    @Override
    public int compareTo(Lsn other) {
        Objects.requireNonNull(other, "Cannot compare against null Lsn");
        return Long.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Lsn)) {
            return false;
        }
        Lsn other = (Lsn) obj;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        if (this.value == INVALID_VAL) {
            return "LSN[INVALID]";
        }
        return "LSN[" + value + "]";
    }
}