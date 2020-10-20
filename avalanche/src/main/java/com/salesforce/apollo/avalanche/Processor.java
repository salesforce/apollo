/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.protocols.HashKey;

public interface Processor {

    class TimedProcessor implements Processor {
        public static class PendingTransaction {
            public final CompletableFuture<HashKey> pending;
            public final ScheduledFuture<?>         timer;

            public PendingTransaction(CompletableFuture<HashKey> pending, ScheduledFuture<?> timer) {
                this.pending = pending;
                this.timer = timer;
            }

            public void complete(HashKey key) {
                log.trace("Finalizing transaction: {}", key);
                timer.cancel(true);
                if (pending != null) {
                    pending.complete(key);
                }
            }

        }

        private final static Logger log = LoggerFactory.getLogger(TimedProcessor.class);

        private Avalanche                                        avalanche;
        private final ConcurrentMap<HashKey, PendingTransaction> pendingTransactions = new ConcurrentHashMap<>();

        /**
         * Create the genesis block for this view
         * 
         * @param data    - the genesis transaction content
         * @param timeout -how long to wait for finalization of the transaction
         * @return a CompleteableFuture indicating whether the transaction is finalized
         *         or not, or whether an exception occurred that prevented processing.
         *         The returned HashKey is the hash key of the the finalized genesis
         *         transaction in the DAG
         */
        public CompletableFuture<HashKey> createGenesis(byte[] data, Duration timeout,
                                                        ScheduledExecutorService scheduler) {
            CompletableFuture<HashKey> futureSailor = new CompletableFuture<>();
            HashKey key = avalanche.submitGenesis(data);
            log.info("Genesis added: {}", key);
            pendingTransactions.put(key, new PendingTransaction(futureSailor,
                    scheduler.schedule(() -> timeout(key), timeout.toMillis(), TimeUnit.MILLISECONDS)));
            return futureSailor;
        }

        @Override
        public void fail(HashKey key) {
            PendingTransaction pending = pendingTransactions.remove(key);
            if (pending != null) {
                pending.pending.completeExceptionally(new TransactionRejected(
                        "Transaction rejected due to conflict resolution"));
            }
        }

        @Override
        public void finalize(HashKey key) {
            PendingTransaction pending = pendingTransactions.remove(key);
            if (pending != null) {
                pending.complete(key);
            }
        }

        public Avalanche getAvalanche() {
            return avalanche;
        }

        public ConcurrentMap<HashKey, PendingTransaction> getPendingTransactions() {
            return pendingTransactions;
        }

        public void setAvalanche(Avalanche avalanche) {
            this.avalanche = avalanche;
        }

        /**
         * Submit a transaction to the group.
         * 
         * @param data    - the transaction content
         * @param timeout -how long to wait for finalization of the transaction
         * @param future  - optional future to be notified of finalization
         * @return the HashKey of the transaction, null if invalid
         */
        public HashKey submitTransaction(HashKey description, byte[] data, Duration timeout,
                                         CompletableFuture<HashKey> future, ScheduledExecutorService scheduler) {
            HashKey key = avalanche.submitTransaction(description, data);
            if (future != null) {
                pendingTransactions.put(key, new PendingTransaction(future,
                        scheduler.schedule(() -> timeout(key), timeout.toMillis(), TimeUnit.MILLISECONDS)));
            }
            return key;
        }

        public CompletableFuture<HashKey> submitTransaction(HashKey description, byte[] data, Duration timeout,
                                                            ScheduledExecutorService scheduler) {
            CompletableFuture<HashKey> future = new CompletableFuture<>();
            submitTransaction(description, data, timeout, future, scheduler);
            return future;
        }

        /**
         * Timeout the pending transaction
         * 
         * @param key
         */
        private void timeout(HashKey key) {
            PendingTransaction pending = pendingTransactions.remove(key);
            if (pending == null) {
                return;
            }
            pending.timer.cancel(true);
            pending.pending.completeExceptionally(new TimeoutException("Transaction timeout"));
        }

    }

    void fail(HashKey txn);

    void finalize(HashKey txn);

}
