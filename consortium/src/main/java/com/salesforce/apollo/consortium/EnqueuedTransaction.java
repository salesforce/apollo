/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesforce.apollo.consortium.TickScheduler.Timer;
import com.salesforce.apollo.protocols.HashKey;

public class EnqueuedTransaction {
    private final HashKey     hash;
    private volatile boolean  timedOut = false;
    private volatile Timer    timer;
    private final Transaction transaction;

    public EnqueuedTransaction(HashKey hash, Transaction transaction) {
        assert hash != null : "requires non null hash";
        this.hash = hash;
        this.transaction = transaction;
    }

    public void cancel() {
        Timer c = timer;
        if (c != null) {
            c.cancel();
        }
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

    public long getDelay() {
        Timer current = timer;
        if (current == null) {
            return Integer.MAX_VALUE;
        }
        return timer.getDelay();
    }

    public HashKey getHash() {
        return hash;
    }

    public Timer getTimer() {
        final Timer c = timer;
        return c;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    @Override
    public int hashCode() {
        return hash.hashCode();
    }

    public boolean isTimedOut() {
        final boolean c = timedOut;
        return c;
    }

    public void setTimedOut(boolean timedOut) {
        this.timedOut = timedOut;
    }

    public void setTimer(Timer timer) {
        this.timer = timer;
    }

    @Override
    public String toString() {
        return "txn " + hash;
    }

    public int totalByteSize() {
        return transaction.getSerializedSize() + 32;
    }
}
