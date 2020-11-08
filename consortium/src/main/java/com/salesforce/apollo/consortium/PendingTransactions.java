/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesforce.apollo.protocols.HashKey;

/**
 * A linked list of unique transactions. This preserves ordering via linked
 * lists of txns, so one should note that the enqueued transaction is literally
 * part of the data structure, meaning that transactions can't be enqueued in
 * the different collections.  Not thread safe
 * 
 * @author hal.hildebrand
 *
 */
public class PendingTransactions implements Iterable<PendingTransactions.EnqueuedTransaction> {
    public static class EnqueuedTransaction {
        private final HashKey                hash;
        private volatile EnqueuedTransaction next;
        private volatile EnqueuedTransaction prev;
        private final Transaction            transaction;

        public EnqueuedTransaction(HashKey hash, Transaction transaction) {
            assert hash != null : "requires non null hash";
            this.hash = hash;
            this.transaction = transaction;
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

        public HashKey getHash() {
            return hash;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        @Override
        public int hashCode() {
            return hash.hashCode();
        }

        @Override
        public String toString() {
            return "txn " + hash;
        }
    }

    private final static Logger log = LoggerFactory.getLogger(PendingTransactions.class);

    private volatile EnqueuedTransaction            head         = null;
    private volatile EnqueuedTransaction            tail         = null;
    private final Map<HashKey, EnqueuedTransaction> transactions = new HashMap<>();

    public boolean add(EnqueuedTransaction e) {
        AtomicBoolean updated = new AtomicBoolean();
        transactions.computeIfAbsent(e.hash, k -> {
            log.trace("Adding pending txn: {}", e.hash);
            EnqueuedTransaction prevHead = head;
            EnqueuedTransaction prevTail = tail;
            if (prevHead == null) {
                head = e;
                tail = e;
            } else {
                prevTail.next = e;
                e.prev = prevTail;
                tail = e;
            }
            updated.set(true);
            return e;
        });
        return updated.get();
    }

    public void clear() {
        head = tail = null;
        transactions.clear();
    }

    public boolean contains(EnqueuedTransaction o) {
        return transactions.containsKey(o.hash);
    }

    public boolean isEmpty() {
        return transactions.isEmpty();
    }

    @Override
    public Iterator<EnqueuedTransaction> iterator() {
        return new Iterator<EnqueuedTransaction>() {
            private EnqueuedTransaction current = head;

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public EnqueuedTransaction next() {
                EnqueuedTransaction next = current;
                if (next == null) {
                    throw new NoSuchElementException();
                }
                current = next.next;
                return next;
            }
        };
    }

    public void remove(Collection<EnqueuedTransaction> txns) {
        txns.forEach(e -> unlink(e));
    }

    public void remove(EnqueuedTransaction txn) {
        unlink(txn);
    }

    public EnqueuedTransaction removeFirst() {
        EnqueuedTransaction prevHead = head;
        if (prevHead == null) {
            return null;
        }
        unlink(prevHead);
        return prevHead;
    }

    public int size() {
        return transactions.size();
    }

    @Override
    public String toString() {
        return "PendingTransactions [head=" + head + ", tail=" + tail + ", transactions=" + transactions + "]";
    }

    private void unlink(EnqueuedTransaction e) {
        if (e.prev != null) {
            e.prev.next = e.next;
        }
        EnqueuedTransaction currentHead = head;
        if (e.equals(currentHead)) {
            head = currentHead.next;
        }
        EnqueuedTransaction currentTail = tail;
        if (e.equals(currentTail)) {
            tail = currentHead.prev;
        }
        head = tail = null;
        transactions.remove(e.hash);

    }

}
