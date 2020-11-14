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

import com.google.protobuf.ByteString;
import com.salesforce.apollo.consortium.Consortium.CollaboratorContext;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;

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
    }

    private final CollaboratorContext                       collaborator;
    private final Deque<EvaluatedTransaction>               evaluated;
    private final AtomicBoolean                             running = new AtomicBoolean();
    private volatile int                                    totalByteSize;
    private final Deque<EnqueuedTransaction>                transactions;
    private final Function<EnqueuedTransaction, ByteString> validator;

    public TransactionSimulator(int bufferSize, CollaboratorContext collaborator,
            Function<EnqueuedTransaction, ByteString> validator) {
        transactions = new LinkedBlockingDeque<>(bufferSize);
        evaluated = new LinkedBlockingDeque<>(bufferSize);
        this.collaborator = collaborator;
        this.validator = validator;
    }

    public boolean add(EnqueuedTransaction transaction) {
        if (!transactions.add(transaction)) {
            return false;
        }
        totalByteSize += transaction.totalByteSize();
        evaluateNext();
        return true;
    }

    public void evaluateNext() {
        boolean started = running.compareAndExchange(false, true);
        if (!started) {
            ForkJoinPool.commonPool().execute(() -> evaluate());
        }
    }

    public int size() {
        return transactions.size();
    }

    private void evaluate() {
        EnqueuedTransaction txn = transactions.peek();
        while (txn != null) {
            ByteString result = validator.apply(txn);
            if (evaluated.offer(new EvaluatedTransaction(txn, result))) {
                transactions.remove();
                totalByteSize += txn.totalByteSize();
                totalByteSize += result.size();
                txn = transactions.peek();
            } else {
                txn = null;
            }
            collaborator.evaluate();
        }
        running.set(false);
        collaborator.drainPending(transactions);
        evaluateNext();
    }

    public int totalByteSize() {
        final int c = totalByteSize;
        return c;
    }

    public int evaluated() {
        return evaluated.size();
    }

}
