/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesforce.apollo.protocols.HashKey;

/**
 * A linked list of unique transactions
 * 
 * @author hal.hildebrand
 *
 */
public class PendingTransactions implements Iterable<PendingTransactions.EnqueuedTransaction> {
    public static class EnqueuedTransaction {
        final HashKey                hash;
        volatile EnqueuedTransaction next;
        final Transaction            transaction;

        public EnqueuedTransaction(HashKey hash, Transaction transaction) {
            assert hash != null : "requires non null hash";
            this.hash = hash;
            this.transaction = transaction;
        }

        @Override
        public String toString() {
            return "txn " + hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            EnqueuedTransaction other = (EnqueuedTransaction) obj;
            return hash.equals(other.hash);
        }

        @Override
        public int hashCode() {
            return hash.hashCode();
        }
    }

    private final static Logger log = LoggerFactory.getLogger(PendingTransactions.class);

    private volatile PendingTransactions.EnqueuedTransaction            head         = null;
    private volatile PendingTransactions.EnqueuedTransaction            tail         = null;
    private final Map<HashKey, PendingTransactions.EnqueuedTransaction> transactions = new ConcurrentHashMap<>();

    public boolean add(PendingTransactions.EnqueuedTransaction e) {
        AtomicBoolean updated = new AtomicBoolean();
        transactions.computeIfAbsent(e.hash, k -> {
            log.trace("Adding pending txn: {}", e.hash);
            PendingTransactions.EnqueuedTransaction prevHead = head;
            PendingTransactions.EnqueuedTransaction prevTail = tail;
            if (prevHead == null) {
                head = e;
                tail = e;
            } else {
                prevTail.next = e;
            }
            updated.set(true);
            return e;
        });
        return updated.get();
    }

    public boolean isEmpty() {
        return transactions.isEmpty();
    }

    @Override
    public String toString() {
        return "PendingTransactions [head=" + head + ", tail=" + tail + ", transactions=" + transactions + "]";
    }

    public void clear() {
        head = tail = null;
        transactions.clear();
    }

    public EnqueuedTransaction removeFirst() {
        EnqueuedTransaction currentHead = head;
        if (currentHead == null) {
            return null;
        }
        head = currentHead.next;
        currentHead.next = null;
        return currentHead;
    }

    public boolean contains(EnqueuedTransaction o) {
        return transactions.containsKey(o.hash);
    }

    @Override
    public Iterator<PendingTransactions.EnqueuedTransaction> iterator() {
        return new Iterator<PendingTransactions.EnqueuedTransaction>() {
            private PendingTransactions.EnqueuedTransaction current = head;

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public PendingTransactions.EnqueuedTransaction next() {
                PendingTransactions.EnqueuedTransaction next = current;
                if (next == null) {
                    throw new NoSuchElementException();
                }
                current = next.next;
                return next;
            }
        };
    }

    public int size() {
        return transactions.size();
    }

}
