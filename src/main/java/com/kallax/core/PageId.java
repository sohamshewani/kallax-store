package com.kallax.core;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a unique identifier for a 4KB fixed-size memory page within the Kallax storage engine.
 * A PageId acts as the primary key mapping logical storage blocks to frames in the Buffer Pool Manager.
 * 
 * Production Standards Enforced:
 * - Immutable value object pattern for thread-safety and cache-friendly hash routing.
 * - Explicit boundary validation preventing invalid allocations or negative offsets.
 * - Optimized memory footprint via primitive fields and efficient hash/equals contracts.
 */
public final class PageId implements Serializable, Comparable<PageId> {

    private static final long serialVersionUID = 1L;

    /**
     * Sentinel value representing an invalid or unallocated page identifier.
     */
    public static final PageId INVALID_PAGE_ID = new PageId(-1);

    private final int id;

    /**
     * Constructs a new PageId with the specified underlying integer identifier.
     * 
     * @ingroup Data Structures & Memory Layout
     * @param id The logical page number. Must be greater than or equal to 0 for valid pages.
     * @throws IllegalArgumentException if the provided id is negative (except for INVALID_PAGE_ID).
     */
    public PageId(int id) {
        if (id < 0 && id != -1) {
            throw new IllegalArgumentException("PageId cannot be negative unless it is the sentinel INVALID_PAGE_ID. Provided: " + id);
        }
        this.id = id;
    }

    /**
     * Retrieves the raw integer value of the PageId.
     * 
     * @return The underlying integer identifier.
     */
    public int getId() {
        return id;
    }

    /**
     * Determines whether this PageId represents a valid, allocatable storage page.
     * 
     * @return true if the id is non-negative, false otherwise.
     */
    public boolean isValid() {
        return this.id >= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PageId pageId = (PageId) o;
        return id == pageId.id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }

    @Override
    public int compareTo(PageId other) {
        Objects.requireNonNull(other, "Cannot compare PageId to null");
        return Integer.compare(this.id, other.id);
    }

    @Override
    public String toString() {
        return "PageId{id=" + id + "}";
    }
}