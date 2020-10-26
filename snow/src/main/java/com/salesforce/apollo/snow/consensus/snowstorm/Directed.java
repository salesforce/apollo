/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class Directed extends Common implements Consensus {
    public static class directedTx extends snowball {

        // ins is the set of txIDs that this tx conflicts with that are less
        // preferred than this tx
        final Set<ID> ins = new HashSet<>();

        // outs is the set of txIDs that this tx conflicts with that are more
        // preferred than this tx
        final Set<ID> outs = new HashSet<>();

        // pendingAccept identifies if this transaction has been marked as accepted
        // once its transitive dependencies have also been accepted
        boolean pendingAccept;

        // tx is the actual transaction this node represents
        Tx tx;

        public directedTx(Tx tx) {
            this.tx = tx;
        }
    }

    private final Map<ID, directedTx> txs   = new HashMap<>();
    private final Map<ID, Set<ID>>    utxos = new HashMap<>();

    public Directed(Context ctx, Parameters params) {
        super(ctx, params);
    }

    @Override
    public void accept(ID txID) {
        // We are accepting the tx, so we should remove the node from the graph.
        directedTx txNode = txs.remove(txID);

        // This tx is consuming all the UTXOs from its inputs, so we can prune them
        // all from memory
        for (ID inputID : txNode.tx.inputIDs()) {
            utxos.remove(inputID);
        }

        // This tx is now accepted, so it shouldn't be part of the virtuous set or
        // the preferred set. Its status as Accepted implies these descriptions.
        virtuous.remove(txID);
        preferences.remove(txID);

        // Reject all the txs that conflicted with this tx.
        reject(txNode.ins);
        // While it is typically true that a tx this is being accepted is preferred,
        // it is possible for this to not be the case. So this is handled for
        // completeness.
        reject(txNode.outs);
        acceptTx(txNode.tx);
    }

    @Override
    public void add(Tx tx) {
        boolean shouldVote = shouldVote(this, tx);
        if (!shouldVote) {
            return;
        }

        ID txID = tx.id();
        directedTx txNode = new directedTx(tx);

        // For each UTXO consumed by the tx:
        // * Add edges between this tx and txs that consume this UTXO
        // * Mark this tx as attempting to consume this UTXO
        for (ID inputID : tx.inputIDs()) {
            // Get the set of txs that are currently processing that also consume
            // this UTXO
            Set<ID> spenders = utxos.getOrDefault(inputID, new HashSet<>());

            // Add all the txs that spend this UTXO to this txs conflicts. These
            // conflicting txs must be preferred over this tx. We know this because
            // this tx currently has a bias of 0 and the tie goes to the tx whose
            // bias was updated first.
            txNode.outs.addAll(spenders);

            // Update txs conflicting with tx to account for its issuance
            for (ID conflictID : spenders) {
                // Get the node that contains this conflicting tx
                directedTx conflict = txs.get(conflictID);

                // This conflicting tx can't be virtuous anymore. So, we attempt to
                // remove it from all of the virtuous sets.
                virtuous.remove(conflictID);
                virtuousVoting.remove(conflictID);

                // This tx should be set to rogue if it wasn't rogue before.
                conflict.rogue = true;

                // This conflicting tx is preferred over the tx being inserted, as
                // described above. So we add the conflict to the inbound set.
                conflict.ins.add(txID);
            }

            // Add this tx to list of txs consuming the current UTXO
            spenders.add(txID);

            // Because this isn't a pointer, we should re-map the set.
            utxos.put(inputID, spenders);
        }

        // Mark this transaction as rogue if had any conflicts registered above
        txNode.rogue = txNode.outs.isEmpty();

        if (!txNode.rogue) {
            // If this tx is currently virtuous, add it to the virtuous sets
            virtuous.add(txID);
            virtuousVoting.add(txID);

            // If a tx is virtuous, it must be preferred.
            preferences.add(txID);
        }

        // Add this tx to the set of currently processing txs
        txs.put(txID, txNode);

        // If a tx that this tx depends on is rejected, this tx should also be
        // rejected.
        registerRejector(this, tx);
    }

    @Override
    public Set<ID> conflicts(Tx tx) {
        Set<ID> conflicts = new HashSet<ID>();
        directedTx node = txs.get(tx.id());
        if (node != null) {
            // If the tx is currently processing, the conflicting txs are just the
            // union of the inbound conflicts and the outbound conflicts.
            conflicts.addAll(node.ins);
            conflicts.addAll(node.outs);
        } else {
            // If the tx isn't currently processing, the conflicting txs are the
            // union of all the txs that spend an input that this tx spends.
            for (ID input : tx.inputIDs()) {
                Set<ID> spends = utxos.get(input);
                if (spends != null) {
                    conflicts.addAll(spends);
                }
            }
        }
        return conflicts;
    }

    @Override
    public boolean issued(Tx tx) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Set<ID> preferences() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void reject(Collection<ID> rejected) {
        for (ID conflictID : rejected) {
            directedTx conflict = txs.get(conflictID);

            // This tx is no longer an option for consuming the UTXOs from its
            // inputs, so we should remove their reference to this tx.
            for (ID inputID : conflict.tx.inputIDs()) {
                Set<ID> txIDs = utxos.get(inputID);
                if (txIDs == null) {
                    // This UTXO may no longer exist because it was removed due to
                    // the acceptance of a tx. If that is the case, there is nothing
                    // left to remove from memory.
                    continue;
                }
                txIDs.remove(conflictID);
                if (txIDs.isEmpty()) {
                    // If this tx was the last tx consuming this UTXO, we should
                    // prune the UTXO from memory entirely.
                    utxos.remove(inputID);
                } else {
                    // If this UTXO still has txs consuming it, then we should make
                    // sure this update is written back to the UTXOs map.
                    utxos.put(inputID, txIDs);
                }
            }

            // We are rejecting the tx, so we should remove it from the graph
            txs.remove(conflictID);

            // While it's statistically unlikely that something being rejected is
            // preferred, it is handled for completion.
            preferences.remove(conflictID);

            // remove the edge between this node and all its neighbors
            removeConflict(conflictID, conflict.ins);
            removeConflict(conflictID, conflict.outs);

            rejectTx(conflict.tx);
        }
    }

    public void removeConflict(ID txID, Collection<ID> neighborIDs) {
        for (ID neighborID : neighborIDs) {
            directedTx neighbor = txs.get(neighborID);
            if (neighbor == null) {
                // If the neighbor doesn't exist, they may have already been
                // rejected, so this mapping can be skipped.
                continue;
            }

            // Remove any edge to this tx.
            neighbor.ins.remove(txID);
            neighbor.outs.remove(txID);

            if (neighbor.outs.isEmpty()) {
                // If this tx should now be preferred, make sure its status is
                // updated.
                preferences.add(neighborID);
            }
        }
    }
}
