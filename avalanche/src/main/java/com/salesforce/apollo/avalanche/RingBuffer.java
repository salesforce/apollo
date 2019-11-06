/**
 * Copyright Hal Hildebrand
 * 
 */
package com.salesforce.apollo.avalanche;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides a fixed size Queue implementation. This class is thread safe.
 *
 * @author hhildebrand
 */
public class RingBuffer<T> extends AbstractQueue<T> {

    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final T[]     items;
    private int           capacity;
    private int           head = 0;
    private int           size = 0;
    private int           tail = 0;

    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        this.capacity = capacity;
        items = (T[]) new Object[capacity];
    }

    public T at(int index) {
        lock.readLock().lock();
        try {
            if (index >= size()) {
                throw new ArrayIndexOutOfBoundsException(String.format("%s >= %s", index, size()));
            }
            return items[(index + head) % items.length];
        } finally {
            lock.readLock().unlock();
        }
    }

    public int drain(T[] into, int from, int max, int to) {
        lock.writeLock().lock();
        try {
            int current = from;
            int i = to;
            int count = 0;
            while (count++ < max && size > 0) {
                into[i++] = items[(current++ + head) % items.length];
            }
            return count;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.AbstractCollection#iterator()
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            int current = 0;

            @Override
            public boolean hasNext() {
                lock.readLock().lock();
                try {
                    return current < size;
                } finally {
                    lock.readLock().unlock();
                }
            }

            @Override
            public T next() {
                lock.readLock().lock();
                try {
                    if (current == size) {
                        throw new NoSuchElementException();
                    }
                    return items[(current++ + head) % items.length];
                } finally {
                    lock.readLock().unlock();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Queue#offer(java.lang.Object)
     */
    @Override
    public boolean offer(T value) {
        if (size == capacity) {
            return false;
        }
        items[tail] = value;
        tail = (tail + 1) % items.length;
        size++;
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Queue#peek()
     */
    @Override
    public T peek() {
        lock.writeLock().lock();
        try {
            if (size == 0) {
                return null;
            }
            return items[head];
        } finally {
            lock.writeLock().unlock();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Queue#poll()
     */
    @Override
    public T poll() {
        lock.readLock().lock();
        try {
            if (size == 0) {
                return null;
            }
            T item = items[head];
            items[head] = null;
            size--;
            head = (head + 1) % items.length;
            return item;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            final StringBuilder buf = new StringBuilder();
            buf.append("[ ");
            for (int i = 0; i < size; i++) {
                buf.append(items[(i + head) % items.length]);
                buf.append(", ");
            }
            buf.append("]");
            return buf.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}
