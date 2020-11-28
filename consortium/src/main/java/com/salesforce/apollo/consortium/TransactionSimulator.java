/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.Deque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
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
    private final AtomicBoolean                             running = new AtomicBoolean();
    private final AtomicBoolean                             started = new AtomicBoolean();
    private volatile int                                    totalByteSize;
    private final Deque<EnqueuedTransaction>                transactions;
    private final Function<EnqueuedTransaction, ByteString> validator;

    public TransactionSimulator(int maxByteSize, CollaboratorContext collaborator, int maxBufferSize,
            Function<EnqueuedTransaction, ByteString> validator) {
        this.bufferSize = maxBufferSize;
        this.maxByteSize = maxByteSize;
        transactions = new LinkedBlockingDeque<>(bufferSize + 1);
        evaluated = new LinkedBlockingDeque<>(bufferSize);
        this.collaborator = collaborator;
        this.validator = validator;
    }

    public boolean add(EnqueuedTransaction transaction) {
        if (!transactions.add(transaction)) {
            return false;
        }
        if (transactions.size() >= bufferSize) {
            return false;
        }
        totalByteSize += transaction.totalByteSize();
        evaluateNext();
        return true;
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

    private void evaluate() {
        EnqueuedTransaction txn = transactions.peek();
        ByteString result = null;
        while (started.get() && txn != null) {
            try {
                log.info("Evaluating transaction: {}", txn.getHash());
                result = validator.apply(txn);
                EvaluatedTransaction eval = new EvaluatedTransaction(txn, result);
                if (evaluated.offer(eval)) {
                    transactions.remove();
                    totalByteSize += txn.totalByteSize();
                    totalByteSize += result.size();
                    txn = transactions.peek();
                } else {
                    transactions.addFirst(txn);
                    log.info("Draining pending from: {}", txn.getHash());
                    collaborator.drainPending();
                    txn = null;
                }
            } catch (Throwable e) {
                log.error("Unable to evaluate transactiion {}", txn.getHash(), e);
                transactions.remove();
                txn = transactions.peek();
            }
        }
        running.set(false);
        evaluateNext();
    }

    private void evaluateNext() {
        boolean evaluating = running.compareAndExchange(false, true);
        if (!evaluating) {
            ForkJoinPool.commonPool().execute(() -> evaluate());
        }
    }

}
