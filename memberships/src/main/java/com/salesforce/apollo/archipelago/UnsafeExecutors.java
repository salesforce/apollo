package com.salesforce.apollo.archipelago;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodType.methodType;

@SuppressWarnings("unused")
public class UnsafeExecutors {
    private static final MethodHandle SET_EXECUTOR;

    static {
        try {
            var unsafeClass = Class.forName("sun.misc.Unsafe");
            var unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            var unsafe = unsafeField.get(null);
            var objectFieldOffset = unsafeClass.getMethod("objectFieldOffset", Field.class);
            var executorField = VTB.class.getDeclaredField("executor");
            executorField.setAccessible(true);
            var executorOffset = (long) objectFieldOffset.invoke(unsafe, executorField);
            var putObject = MethodHandles.lookup()
                                         .findVirtual(unsafeClass, "putObject",
                                                      methodType(void.class, Object.class, long.class, Object.class));
            var setExecutor = insertArguments(insertArguments(putObject, 2, executorOffset), 0, unsafe);
            SET_EXECUTOR = setExecutor;
        } catch (ClassNotFoundException | NoSuchFieldException | NoSuchMethodException | IllegalAccessException |
        InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }

    public static ExecutorService newVirtualThreadPerTaskExecutor() {
        return virtualThreadExecutor(Executors.newWorkStealingPool());
    }

    public static <B extends Thread.Builder> B configureBuilderExecutor(B builder, Executor executor) {
        if (executor != null) {
            setExecutor(builder, executor);
        }
        return builder;
    }

    public static ExecutorService virtualThreadExecutor(ExecutorService executor) {
        Objects.requireNonNull(executor);
        return new VirtualThreadExecutor(executor);
    }

    private static void setExecutor(Object builder, Object executor) {
        try {
            SET_EXECUTOR.invokeExact(builder, executor);
        } catch (Throwable e) {
            throw new AssertionError(e);
        }
    }

    public static ThreadPoolExecutor newCachedThreadPool(int corePoolSize) {
        return newCachedThreadPool(corePoolSize, true);
    }

    public static ThreadPoolExecutor newCachedThreadPool(int corePoolSize, boolean prestart) {
        var executorService = newCachedThreadPool(corePoolSize, new ForkJoinPool());
        if (prestart) {
            executorService.prestartAllCoreThreads();
        }
        return executorService;
    }

    public static ThreadPoolExecutor newCachedThreadPool(int corePoolSize, ExecutorService executor) {
        ThreadFactory factory = r -> {
            var builder = Thread.ofVirtual();
            setExecutor(builder, executor);
            return builder.unstarted(r);
        };
        return new ThreadPoolExecutor(corePoolSize, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>(), factory) {
            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return executor.awaitTermination(timeout, unit);
            }

            @Override
            public boolean isShutdown() {
                return executor.isShutdown();
            }

            @Override
            public boolean isTerminated() {
                return executor.isTerminated() && super.isTerminated();
            }

            @Override
            public void shutdown() {
                executor.shutdown();
                super.shutdown();
            }

            @Override
            public List<Runnable> shutdownNow() {
                var returned = executor.shutdownNow();
                super.shutdownNow();
                return returned;
            }
        };
    }

    public static ExecutorService newFixedThreadPool(int nThreads, ExecutorService executor) {
        ThreadFactory factory = r -> {
            var builder = Thread.ofVirtual();
            setExecutor(builder, executor);
            return builder.unstarted(r);
        };
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                                      new LinkedBlockingQueue<Runnable>(), factory) {
            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return executor.awaitTermination(timeout, unit);
            }

            @Override
            public boolean isShutdown() {
                return executor.isShutdown();
            }

            @Override
            public boolean isTerminated() {
                return executor.isTerminated() && super.isTerminated();
            }

            @Override
            public void shutdown() {
                executor.shutdown();
                super.shutdown();
            }

            @Override
            public List<Runnable> shutdownNow() {
                var returned = executor.shutdownNow();
                super.shutdownNow();
                return returned;
            }
        };
    }

    private static class BTB {
        private int                      characteristics;
        private long                     counter;
        private String                   name;
        private UncaughtExceptionHandler uhe;
    }

    private static class VirtualThreadExecutor extends AbstractExecutorService {
        private final ExecutorService executor;
        private final AtomicBoolean   started = new AtomicBoolean(true);

        public VirtualThreadExecutor(ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            if (!started.get()) {
                throw new RejectedExecutionException("Executor shutdown");
            }
            var builder = Thread.ofVirtual();
            setExecutor(builder, executor);
            builder.start(command);
        }

        @Override
        public boolean isShutdown() {
            return executor.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return !executor.isTerminated();
        }

        @Override
        public void shutdown() {
            if (!started.compareAndSet(true, false)) {
                return;
            }
            executor.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            if (!started.compareAndSet(true, false)) {
                return List.of();
            }
            return executor.shutdownNow();
        }
    }

    private static class VTB extends BTB {
        private Executor executor;
    }
}
