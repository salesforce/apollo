package com.salesforce.apollo.context;

import com.salesforce.apollo.utils.Pair;

import java.util.Comparator;

/**
 * StaticSearch is used to search in an Array of type T comparisons are done using K which can be extracted from T
 *
 * @param <T> vector type
 * @param <K> the key used for sorting to extract from T and to compare
 * @author lindenb
 */
public abstract class StaticSearch<T, K> {
    private final Comparator<K> comparator;

    /**
     * Search
     *
     * @param comparator used to compare two keys of type K
     */
    public StaticSearch(Comparator<K> comparator) {
        this.comparator = comparator;
    }

    /** C+ equals range */
    public Pair<Integer, Integer> equal_range(T[] dataVector, K select) {
        return equal_range(dataVector, 0, dataVector.length, select);
    }

    /**
     * C+ equals range
     *
     * @param bounds array of two integers [begin; end]
     * @param select the value to search
     */
    public Pair<Integer, Integer> equal_range(T[] dataVector, Pair<Integer, Integer> bounds, K select) {
        return equal_range(dataVector, bounds.a(), bounds.b(), select);
    }

    /** C+ equals range */
    public Pair<Integer, Integer> equal_range(T[] dataVector, int first, int last, K subject) {
        int left = lower_bound(dataVector, first, last, subject);
        int right = upper_bound(dataVector, left, last, subject);
        return new Pair<Integer, Integer>(left, right);
    }

    /** @return the internal comparator */
    public Comparator<K> getComparator() {
        return this.comparator;
    }

    /** method used to extract the key(K) from an object (T) */
    public abstract K getKey(T value);

    /** @return True if the vector is sorted */
    public boolean isSorted(T[] dataVector) {
        return isSorted(dataVector, 0, dataVector.length);
    }

    /** @return True if the vector is sorted between begin and end */
    public boolean isSorted(T[] dataVector, int begin, int end) {
        while (begin + 1 < end) {
            if (this.comparator.compare(getKey(dataVector[begin]), getKey(dataVector[begin + 1])) > 0) {
                return false;
            }
            ++begin;
        }
        return true;
    }

    /** C+ lower_bound */
    public int lower_bound(T[] dataVector, K select) {
        return lower_bound(dataVector, 0, dataVector.length, select);
    }

    /** C+ lower_bound */
    public int lower_bound(T[] dataVector, int first, int last, K select) {
        int len = last - first;
        while (len > 0) {
            int half = len / 2;
            int middle = first + half;
            T x = dataVector[middle];
            if (this.comparator.compare(getKey(x), select) < 0) {
                first = middle + 1;
                len -= half + 1;
            } else {
                len = half;
            }
        }
        return first;
    }

    @Override
    public String toString() {
        return "Algorithm(" + this.comparator + ")";
    }

    /** C+ upper_bound */
    public int upper_bound(T[] dataVector, K select) {
        return upper_bound(dataVector, 0, dataVector.length, select);
    }

    /** C+ upper_bound */
    public int upper_bound(T[] dataVector, int first, int last, K select) {
        int len = last - first;
        while (len > 0) {
            int half = len / 2;
            int middle = first + half;
            T x = dataVector[middle];
            if (this.comparator.compare(select, getKey(x)) < 0) {
                len = half;
            } else {
                first = middle + 1;
                len -= half + 1;
            }
        }
        return first;
    }
}
