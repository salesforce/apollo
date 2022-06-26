package com.salesforce.apollo.utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchingQueue<T> {
    public class Batch {
        private int           byteSize;
        private final List<T> events = new ArrayList<>();

        public Iterator<T> iterator() {
            return events.iterator();
        }

        @Override
        public String toString() {
            return String.format("Batch [bytes=%s, size=%s]", byteSize, events.size());
        }

        List<T> getEvents() {
            return events;
        }

        private boolean addEvent(T event) {
            if (taken == limit) {
                return false;
            }
            if (events.size() == batchSize) {
                if (!reapCurrentBatch()) {
                    log.trace("rejecting event size: {} added: {} taken: {}", size, added, taken);
                    return false;
                }
            }
            final var eventSize = sizer.apply(event);
            if (byteSize + eventSize > maxByteSize) {
                if (!reapCurrentBatch()) {
                    log.trace("rejecting event size: {} added: {} taken: {}", size, added, taken);
                    return false;
                }
            }

            final var add = events.add(event);
            if (add) {
                size = size + 1;
                byteSize += eventSize;
                log.trace("adding event: {} size: {} added: {} taken: {}", eventSize, size, added, taken);
            }
            return add;

        }

        private void clear() {
            events.clear();
            byteSize = 0;
        }
    }

    private final static Logger log = LoggerFactory.getLogger(BatchingQueue.class);

    private int                                added;
    private final int                          batchSize;
    private final Batch                        currentBatch;
    private final int                          limit;
    private final ReentrantLock                lock;
    private final int                          maxByteSize;
    private final LinkedBlockingQueue<List<T>> oldBatches = new LinkedBlockingQueue<>();
    private int                                size;
    private final Function<T, Integer>         sizer;
    private int                                taken;

    public BatchingQueue(int limit, int batchSize, Function<T, Integer> sizer, int maxByteSize) {
        this.limit = limit;
        this.batchSize = batchSize;
        this.maxByteSize = maxByteSize;
        this.sizer = sizer;
        this.currentBatch = createNewBatch();
        lock = new ReentrantLock();
        added = 1;
        taken = 0;
    }

    public int added() {
        lock.lock();
        try {
            return added;
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();
        try {
            oldBatches.clear();
            currentBatch.clear();
            size = 0;
            added = 1;
            taken = 0;
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(T event) {
        lock.lock();
        try {
            return currentBatch.addEvent(event);
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    public List<T> take(Duration timeout) throws InterruptedException {
        lock.lock();
        try {
            if (taken == limit) {
                log.trace("Batch limit achieved size: {} added: {} taken: {}", size, added, taken);
                return null;
            }
            List<T> batch;
            if (oldBatches.isEmpty() && !currentBatch.events.isEmpty()) {
                batch = new ArrayList<>(currentBatch.events);
                currentBatch.clear();
                size -= batch.size();
                added++;
                taken++;
                log.trace("Taking events: {} new size: {} added: {} taken: {}", batch.size(), size, added, taken);
                return batch;

            }
            taken++;
        } finally {
            lock.unlock();
        }

        List<T> batch = oldBatches.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        lock.lock();
        try {
            if (batch == null) {
                log.trace("No events to take, size: {} added: {} taken: {}", size, added, taken);
                return null;
            }
            size -= batch.size();
            log.trace("Taking events: {} new size: {} added: {} taken: {}", batch.size(), size, added, taken);
            return batch;
        } finally {
            lock.unlock();
        }
    }

    public int taken() {
        lock.lock();
        try {
            return taken;
        } finally {
            lock.unlock();
        }
    }

    Batch getCurrentBatch() {
        return currentBatch;
    }

    private Batch createNewBatch() {
        return new Batch();
    }

    private boolean reapCurrentBatch() {
        if (added >= limit || taken == limit) {
            return false;
        }

        oldBatches.offer(new ArrayList<>(currentBatch.events));
        added++;
        currentBatch.clear();
        return added <= limit;
    }
}
