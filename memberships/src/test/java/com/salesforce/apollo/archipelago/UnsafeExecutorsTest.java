package com.salesforce.apollo.archipelago;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class UnsafeExecutorsTest {
    private static String carrierThreadName() {
        var name = Thread.currentThread().toString();
        var index = name.lastIndexOf('@');
        if (index == -1) {
            throw new AssertionError();
        }
        return name.substring(index + 1);
    }

    @Test
    public void virtualThreadExecutorSingleThreadExecutor() throws InterruptedException {
        var executor = Executors.newSingleThreadExecutor();
        var virtualExecutor = UnsafeExecutors.virtualThreadExecutor(executor);
        var carrierThreadNames = new CopyOnWriteArraySet<String>();
        for (var i = 0; i < 10; i++) {
            virtualExecutor.execute(() -> carrierThreadNames.add(carrierThreadName()));
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        assertEquals(1, carrierThreadNames.size());
    }

    @Test
    void testVirtualThread() {
        Queue<Runnable> executor = new ArrayDeque<>();
        var virtualExecutor = UnsafeExecutors.virtualThreadExecutor(wrap(executor::add));

        Lock lock = new ReentrantLock();
        lock.lock();
        virtualExecutor.execute(lock::lock);
        assertEquals(1, executor.size(), "runnable for vthread has not been submitted");
        executor.poll().run();
        assertEquals(0, executor.size(), "vthread has not blocked");
        lock.unlock();
        assertEquals(1, executor.size(), "vthread is not schedulable");
        executor.poll().run();
        assertFalse(lock.tryLock(), "the virtual thread does not hold the lock");
    }

    private ExecutorService wrap(Executor ex) {
        return new AbstractExecutorService() {

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return false;
            }

            @Override
            public void execute(Runnable command) {
                System.out.println("Yes!");
                ex.execute(command);
            }

            @Override
            public boolean isShutdown() {
                return false;
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public void shutdown() {

            }

            @Override
            public List<Runnable> shutdownNow() {
                return List.of();
            }
        };
    }
}
