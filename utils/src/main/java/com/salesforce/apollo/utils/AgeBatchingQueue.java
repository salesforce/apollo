package com.salesforce.apollo.utils;

import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <ul>
 * <li>This queue maintains a current batch, an instance of
 * {@link AgeBatch}</li>
 * <li>All calls to {@link AgeBatchingQueue#offer(T)} will add the event to this
 * batch.</li>
 * <li>All batches which are aged (crossed the max age) move to a blocking
 * queue.</li>
 * <li>All individual instances of this queue will schedule a single task using
 * the supplied scheduler to deduce the batch age according to the batch age
 * specified in {@link Subscribe}</li>
 * <li>The above task will periodically move the current batch to the old
 * batches queue, mentioned above.</li>
 * <li>In case, the old batch queue is full, the reaper task sets a flag
 * signifying that the queue is full and does <em>NOT</em> reap the current
 * batch.</li>
 * <li>Every subsequent offer to this queue, will try to reap the current batch,
 * failing which, the offer will fail.</li>
 * <li>The failure of above offer will typically make the consumer remove &
 * discard a batch and retry.</li>
 * </ul>
 * 
 * @author Nitesh Kant (nkant@netflix.com)
 */
public class AgeBatchingQueue<T> {
    class AgeBatch {

        ConcurrentLinkedQueue<T> events;

        protected AgeBatch() {
            events = new ConcurrentLinkedQueue<>();
        }

        public Iterator<T> iterator() {
            return events.iterator();
        }

        protected boolean addEvent(T event) {
            return events.add(event);
        }

        protected void clear() {
            events.clear();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AgeBatchingQueue.class);

    private final ReentrantLock                 batchReapingLock;
    private final AtomicReference<AgeBatch>     currentBatch;
    private volatile ScheduledFuture<?>         fs;
    private final String                        label;
    private final LinkedBlockingQueue<AgeBatch> oldBatches;
    private final AtomicBoolean                 oldBatchesQueueFull;
    private final AtomicInteger                 size    = new AtomicInteger();
    private final AtomicBoolean                 started = new AtomicBoolean();

    AgeBatchingQueue(String label, int queueSize) {
        this.label = label;
        oldBatches = new LinkedBlockingQueue<>(queueSize);
        currentBatch = new AtomicReference<>(createNewBatch());
        oldBatchesQueueFull = new AtomicBoolean();
        batchReapingLock = new ReentrantLock();
    }

    public Queue<T> blockingTake() throws InterruptedException {
        AgeBatch batch = oldBatches.take();
        size.decrementAndGet();
        return batch.events;
    }

    public AgeBatch blockingTakeWithTimeout(long timeoutInMillis) throws InterruptedException {
        return oldBatches.poll(timeoutInMillis, TimeUnit.MILLISECONDS);
    }

    public void clear() {
        oldBatches.clear();
        currentBatch.get().clear();
        size.set(0);
    }

    public Queue<T> nonBlockingTake() {
        AgeBatch batch = oldBatches.poll();
        if (null != batch) {
            size.decrementAndGet();
        }
        return batch == null ? null : batch.events;
    }

    public boolean offer(T event) {
        if (oldBatchesQueueFull.get()) {
            if (!reapCurrentBatch("Offering Thread")) {
                return false;
            }
        }
        return currentBatch.get().addEvent(event);
    }

    public int size() {
        return size.get();
    }

    public void start(Duration period, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        var millis = period.toMillis();
        fs = scheduler.scheduleWithFixedDelay(() -> {
            try {
                reapCurrentBatch("Reaper");
            } catch (Throwable th) {
                LOGGER.error("Reaper thread for: {} threw an error while reaping. Eating exception.", label, th);
            }
        }, millis, millis, TimeUnit.MILLISECONDS);

    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        final var futureSailor = fs;
        if (futureSailor != null) {
            fs = null;
            futureSailor.cancel(false);
        }
    }

    AgeBatch createNewBatch() {
        return new AgeBatch();
    }

    AgeBatch getCurrentBatch() {
        return currentBatch.get();
    }

    boolean invokeReaping(String operation) {
        return reapCurrentBatch(operation);
    }

    boolean reapCurrentBatch(String operatorName) {
        AgeBatch currentBatchRef = currentBatch.get();
        if (currentBatchRef.events.isEmpty()) {
            return true;
        }
        // We should not block here as the offer & reaper thread both does not block in
        // any condition.
        if (batchReapingLock.tryLock()) {
            try {
                if (oldBatches.offer(currentBatchRef)) {
                    currentBatch.getAndSet(createNewBatch());
                    size.incrementAndGet();
                    LOGGER.debug("[Reaping source: {}] Reaped the old batch with size {} for: {}", operatorName,
                                 currentBatchRef.events.size(), label);
                    oldBatchesQueueFull.set(false);
                    return true;
                } else {
                    oldBatchesQueueFull.set(true);
                    LOGGER.info("[Reaping source: {}] Old batches queue for: {} is full. Not reaping the batch till we get space.",
                                operatorName, label);
                }
            } finally {
                batchReapingLock.unlock();
            }
        } else {
            LOGGER.debug("[Reaping source: {} ] Subscriber: {} did not reap as there is another thread already reaping.",
                         operatorName, label);
        }
        return false;
    }
}
