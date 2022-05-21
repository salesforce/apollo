/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author hal.hildebrand
 *
 */
public class ListenableCompletableFuture<T> implements ListenableFuture<T> {

    public static <T> ListenableFuture<T> wrap(CompletableFuture<T> wrapped) {
        return new ListenableCompletableFuture<>(wrapped);
    }

    private final CompletableFuture<T> wrapped;

    ListenableCompletableFuture(CompletableFuture<T> wrappedFuture) {
        this.wrapped = wrappedFuture;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        wrapped.thenRunAsync(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return wrapped.cancel(mayInterruptIfRunning);
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return wrapped.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return wrapped.get(timeout, unit);
    }

    @Override
    public boolean isCancelled() {
        return wrapped.isCancelled();
    }

    @Override
    public boolean isDone() {
        return wrapped.isDone();
    }
}
