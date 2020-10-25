/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier= BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https=//opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.choices.Status;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.events.Blocker;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class Common {
    public static class acceptor {
        private Set<ID>               deps = new HashSet<>();
        private final List<Throwable> errs;
        private final Consensus       g;
        private boolean               rejected;
        private final ID              txID;

        public acceptor(Consensus g, List<Throwable> errs, ID txID) {
            this.g = g;
            this.errs = errs;
            this.txID = txID;
        }

        public void abandon(ID id) {
            rejected = true;
        }

        public Set<ID> dependencies() {
            return deps;
        }

        public void fulfill(ID id) {
            deps.remove(id);
            update();
        }

        public void update() {
            if (rejected || !deps.isEmpty() || !errs.isEmpty()) {
                return;
            }
            g.accept(txID);
        }
    }

    public static class rejector {
        boolean                       rejected;
        private final Set<ID>         deps = new HashSet<>();
        private final List<Throwable> errs;
        private final Consensus       g;
        private final ID              txID;

        public rejector(Consensus g, List<Throwable> errs, ID txID) {
            this.g = g;
            this.errs = errs;
            this.txID = txID;
        }

        public void abandon(ID id) {
        }

        public Set<ID> dependencies() {
            return deps;
        }

        public void fulfill(ID id) {
            if (rejected || !errs.isEmpty()) {
                return;
            }
            rejected = true;
            try {
                g.reject(txID);
            } catch (Throwable e) {
                errs.add(e);
            }
        }

        public void update() {

        }
    }

    public static class snowballNode {
        int confidence;
        int numSuccessfulPolls;
        ID  txID;
    }

    protected Context         ctx;
    protected int             currentVote;
    protected List<Throwable> errs;
    protected metrics         metrics;
    protected Parameters      parameters;
    protected Blocker         pendingAccept;
    protected Blocker         pendingReject;
    protected Set<ID>         preferences;
    protected Set<ID>         virtuous;
    protected Set<ID>         virtuousVoting;

    public Common(Context ctx, Parameters params) {
        this.ctx = ctx;
        this.parameters = params;
        params.valid();
    }

    public boolean finalized() {
        int numPreferences = preferences.size();
        ctx.log.trace("Conflict graph has {} preferred transactions", numPreferences);
        return numPreferences == 0;
    }

    public Parameters parameters() {
        return parameters;
    }

    public boolean quiesce() {
        int numVirtuous = virtuousVoting.size();
        ctx.log.trace("Conflict graph has {} voting virtuous transactions", numVirtuous);
        return numVirtuous == 0;
    }

    public Set<ID> virtuous() {
        return virtuous;
    }

    protected void acceptTx(Tx tx) {
        // Accept is called before notifying the IPC so that acceptances that cause
        // fatal errors aren't sent to an IPC peer.
        tx.accept();

        ID txID = tx.id();

        // Notify the IPC socket that this tx has been accepted.
        ctx.decisionDispatcher.accept(ctx, txID, tx.bytes());

        // Update the metrics to account for this transaction's acceptance
        metrics.accepted(txID);

        // If there is a tx that was accepted pending on this tx, the ancestor
        // should be notified that it doesn't need to block on this tx anymore.
        pendingAccept.fulfill(txID);
        // If there is a tx that was issued pending on this tx, the ancestor tx
        // doesn't need to be rejected because of this tx.
        pendingReject.abandon(txID);
    }

    // registerAcceptor attempts to accept this tx once all its dependencies are
    // accepted. If all the dependencies are already accepted, this function will
    // immediately accept the tx.
    protected void registerAcceptor(Consensus con, Tx tx) {
        ID txID = tx.id();
        acceptor toAccept = new acceptor(con, errs, txID);

        for (Tx dependency : tx.dependencies()) {
            if (dependency.status() != Status.ACCEPTED) {
                // If the dependency isn't accepted, then it must be processing.
                // This tx should be accepted after this tx is accepted. Note that
                // the dependencies can't already be rejected, because it is assumed
                // that this tx is currently considered valid.
                toAccept.deps.add(dependency.id());
            }
        }

        // This tx is no longer being voted on, so we remove it from the voting set.
        // This ensures that virtuous txs built on top of rogue txs don't force the
        // node to treat the rogue tx as virtuous.
        virtuousVoting.remove(txID);
        pendingAccept.register(toAccept);
    }

    // registerRejector rejects this tx if any of its dependencies are rejected.
    protected void registerRejector(Consensus con, Tx tx) {
        // If a tx that this tx depends on is rejected, this tx should also be
        // rejected.
        rejector toReject = new rejector(con, errs, tx.id());

        // Register all of this txs dependencies as possibilities to reject this tx.
        for (Tx dependency : tx.dependencies()) {
            if (dependency.status() != Status.ACCEPTED) {
                // If the dependency isn't accepted, then it must be processing. So,
                // this tx should be rejected if any of these processing txs are
                // rejected. Note that the dependencies can't already be rejected,
                // because it is assumed that this tx is currently considered valid.
                toReject.deps.add(dependency.id());
            }
        }

        // Register these dependencies
        pendingReject.register(toReject);
    }

    // reject the provided tx.
    protected void rejectTx(Tx tx) {
        // Reject is called before notifying the IPC so that rejections that
        // cause fatal errors aren't sent to an IPC peer.
        tx.reject();

        ID txID = tx.id();

        // Notify the IPC that the tx was rejected
        ctx.decisionDispatcher.reject(ctx, txID, tx.bytes());

        // Update the metrics to account for this transaction's rejection
        metrics.rejected(txID);

        // If there is a tx that was accepted pending on this tx, the ancestor
        // tx can't be accepted.
        pendingAccept.abandon(txID);
        // If there is a tx that was issued pending on this tx, the ancestor tx
        // must be rejected.
        pendingReject.fulfill(txID);
    }

    // shouldVote returns if the provided tx should be voted on to determine if it
    // can be accepted. If the tx can be vacuously accepted, the tx will be accepted
    // and will therefore not be valid to be voted on.
    protected boolean shouldVote(Consensus con, Tx tx) {
        if (con.issued(tx)) {
            // If the tx was previously inserted, it shouldn't be re-inserted.
            return false;
        }

        ID txID = tx.id();
        byte[] bytes = tx.bytes();

        // Notify the IPC socket that this tx has been issued.
        ctx.decisionDispatcher.issue(ctx, txID, bytes);

        // Notify the metrics that this transaction is being issued.
        metrics.issued(txID);

        // If this tx has inputs, it needs to be voted on before being accepted.
        if (!tx.inputIDs().isEmpty()) {
            return true;
        }

        // Since this tx doesn't have any inputs, it's impossible for there to be
        // any conflicting transactions. Therefore, this transaction is treated as
        // vacuously accepted and doesn't need to be voted on.

        // Accept is called before notifying the IPC so that acceptances that
        // cause fatal errors aren't sent to an IPC peer.
        tx.accept();

        // Notify the IPC socket that this tx has been accepted.
        ctx.decisionDispatcher.accept(ctx, txID, bytes);

        // Notify the metrics that this transaction was just accepted.
        metrics.accepted(txID);
        return false;
    }

}
