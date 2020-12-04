/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.Deque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * @author hal.hildebrand
 *
 */
public class TransactionSimulator {

    public static class EvaluatedTransaction {
        public final ByteString          result;
        public final EnqueuedTransaction transaction;

        public EvaluatedTransaction(EnqueuedTransaction transaction, ByteString result) {
            this.transaction = transaction;
            this.result = result;
        }

        public int getSerializedSize() {
            return transaction.getTransaction().getSerializedSize() + 32;
        }
    }

    private final static Logger log = LoggerFactory.getLogger(TransactionSimulator.class);

    private final int                                       bufferSize;
    private final CollaboratorContext                       collaborator;
    private final Deque<EvaluatedTransaction>               evaluated;
    @SuppressWarnings("unused")
    private final int                                       maxByteSize;
    private final AtomicBoolean                             started = new AtomicBoolean();
    private volatile int                                    totalByteSize;
    private final LinkedBlockingDeque<Runnable>             transactions;
    private final Function<EnqueuedTransaction, ByteString> validator;
    private final ExecutorService                           executor;

    public TransactionSimulator(int maxByteSize, CollaboratorContext collaborator, int maxBufferSize,
            Function<EnqueuedTransaction, ByteString> validator) {
        this.bufferSize = maxBufferSize;
        this.maxByteSize = maxByteSize;
        transactions = new LinkedBlockingDeque<>(bufferSize + 1);
        evaluated = new LinkedBlockingDeque<>(bufferSize);
        this.collaborator = collaborator;
        this.validator = validator;
        new ThreadPoolExecutor(1, 1, Integer.MAX_VALUE, TimeUnit.DAYS, transactions);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "Txn Sim: [" + collaborator.getMember() + "]");
            t.setDaemon(true);
            return t;
        });
    }

    public boolean add(EnqueuedTransaction transaction) {
        if (transactions.size() >= bufferSize) {
            return false;
        }
        Runnable evaluation = () -> {
            if (started.get()) {
                evaluate(transaction);
            }
        };
        try {
            executor.execute(evaluation);
            totalByteSize += transaction.totalByteSize();
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    public int available() {
        return bufferSize - transactions.size() - 1;
    }

    public int evaluated() {
        return evaluated.size();
    }

    public boolean isEmpty() {
        return evaluated.isEmpty();
    }

    public EvaluatedTransaction peek() {
        return evaluated.peek();
    }

    public EvaluatedTransaction poll() {
        return evaluated.poll();
    }

    public int size() {
        return transactions.size();
    }

    public void start() {
        if (!started.compareAndExchange(false, true)) {
            return;
        }
    }

    public void stop() {
        if (started.compareAndExchange(true, false)) {
            return;
        }
        transactions.clear();
        evaluated.clear();
    }

    public int totalByteSize() {
        final int c = totalByteSize;
        return c;
    }

    private void evaluate(final EnqueuedTransaction txn) {
        if (!started.get()) {
            return;
        }

        ByteString result = null;
        try {
            log.debug("Evaluating transaction: {}", txn.getHash());
            result = validator.apply(txn);
            EvaluatedTransaction eval = new EvaluatedTransaction(txn, result);
            if (evaluated.offer(eval)) {
                totalByteSize += txn.totalByteSize();
                totalByteSize += result.size();
            } else {
                transactions.addFirst(() -> evaluate(txn));
                log.debug("Draining pending from: {}", txn.getHash());
                collaborator.drainPending();
            }
        } catch (Throwable e) {
            log.error("Unable to evaluate transactiion {}", txn.getHash(), e);
        }
    }

}
