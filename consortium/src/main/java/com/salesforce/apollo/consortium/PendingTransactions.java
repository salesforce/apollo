/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    private volatile PendingTransactions.EnqueuedTransaction   head         = null;
    private volatile PendingTransactions.EnqueuedTransaction   tail         = null;
    private final Set<PendingTransactions.EnqueuedTransaction> transactions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public boolean add(PendingTransactions.EnqueuedTransaction e) {
        if (transactions.add(e)) {
            log.trace("Adding pending txn: {}", e.hash);
            PendingTransactions.EnqueuedTransaction prevHead = head;
            PendingTransactions.EnqueuedTransaction prevTail = tail;
            if (prevHead == null) {
                head = e;
                tail = e;
            } else {
                prevTail.next = e;
            }
            return true;
        }
        log.trace("Already have seen txn: {}:{}", e.hash, size());
        return false;
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

    public boolean contains(Object o) {
        return transactions.contains(o);
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
