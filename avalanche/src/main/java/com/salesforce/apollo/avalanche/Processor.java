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

import com.google.protobuf.Message;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.avalanche.WorkingSet.FinalizationData;
import com.salesforce.apollo.crypto.Digest;

public interface Processor {

    class NullProcessor implements Processor {

        @Override
        public void finalize(FinalizationData finalized) {
            // do nothing
        }

        @Override
        public Digest validate(Digest key, DagEntry entry) {
            return key;
        }

    }

    class TimedProcessor implements Processor {
        public static class PendingTransaction {
            public final CompletableFuture<Digest> pending;
            public final ScheduledFuture<?>        timer;

            public PendingTransaction(CompletableFuture<Digest> pending, ScheduledFuture<?> timer) {
                this.pending = pending;
                this.timer = timer;
            }

            public void complete(Digest key) {
                log.trace("Finalizing transaction: {}", key);
                timer.cancel(true);
                if (pending != null) {
                    pending.complete(key);
                }
            }

        }

        private final static Logger log = LoggerFactory.getLogger(TimedProcessor.class);

        private Avalanche                                       avalanche;
        private final ConcurrentMap<Digest, PendingTransaction> pendingTransactions = new ConcurrentHashMap<>();

        /**
         * Create the genesis block for this view
         * 
         * @param data    - the genesis transaction content
         * @param timeout -how long to wait for finalization of the transaction
         * @return a CompleteableFuture indicating whether the transaction is finalized
         *         or not, or whether an exception occurred that prevented processing.
         *         The returned Digest is the hash key of the the finalized genesis
         *         transaction in the DAG
         */
        public CompletableFuture<Digest> createGenesis(Message data, Duration timeout,
                                                       ScheduledExecutorService scheduler) {
            CompletableFuture<Digest> futureSailor = new CompletableFuture<>();
            Digest key = avalanche.submitGenesis(data);
            log.info("Genesis added: {}", key);
            pendingTransactions.put(key, new PendingTransaction(futureSailor,
                    scheduler.schedule(() -> timeout(key), timeout.toMillis(), TimeUnit.MILLISECONDS)));
            return futureSailor;
        }

        @Override
        public void finalize(FinalizationData finalized) {
            finalized.finalized.forEach(e -> {
                Digest hash = e.hash;
                PendingTransaction pending = pendingTransactions.remove(hash);
                if (pending != null) {
                    pending.complete(hash);
                }
            });
            finalized.deleted.forEach(e -> {
                PendingTransaction pending = pendingTransactions.remove(e);
                if (pending != null) {
                    pending.pending.completeExceptionally(new TransactionRejected(
                            "Transaction rejected due to conflict resolution"));
                }
            });
        }

        public Avalanche getAvalanche() {
            return avalanche;
        }

        public ConcurrentMap<Digest, PendingTransaction> getPendingTransactions() {
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
         * @return the Digest of the transaction, null if invalid
         */
        public Digest submitTransaction(Message data, Duration timeout, CompletableFuture<Digest> future,
                                        ScheduledExecutorService scheduler) {
            Digest key = avalanche.submitTransaction(data);
            if (future != null) {
                pendingTransactions.put(key, new PendingTransaction(future,
                        scheduler.schedule(() -> timeout(key), timeout.toMillis(), TimeUnit.MILLISECONDS)));
            }
            return key;
        }

        public CompletableFuture<Digest> submitTransaction(Message data, Duration timeout,
                                                           ScheduledExecutorService scheduler) {
            CompletableFuture<Digest> future = new CompletableFuture<>();
            submitTransaction(data, timeout, future, scheduler);
            return future;
        }

        @Override
        public Digest validate(Digest key, DagEntry entry) {
            return key;
        }

        /**
         * Timeout the pending transaction
         * 
         * @param key
         */
        private void timeout(Digest key) {
            PendingTransaction pending = pendingTransactions.remove(key);
            if (pending == null) {
                return;
            }
            pending.timer.cancel(true);
            pending.pending.completeExceptionally(new TimeoutException("Transaction timeout"));
        }

    }

    /**
     * Finalize the transactions
     */
    void finalize(FinalizationData finalized);

    /**
     * Validate the entry.
     * 
     * @return Digest of the conflict set for the entry, or null if invalid.
     */
    Digest validate(Digest key, DagEntry entry);

}
