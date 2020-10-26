/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.choices.Status;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class Input extends Common implements Consensus {
    private static class inputUTXO {
        boolean        rogue;
        Collection<ID> spenders;
        ID             preference;
        ID             color;
        int            confidence;
        int            lastVote;
        int            numSuccessfulPolls;

    }

    private static class inputTx {
        public inputTx(Tx tx) {
            this.tx = tx;
        }

        // pendingAccept identifies if this transaction has been marked as accepted
        // once its transitive dependencies have also been accepted
        boolean pendingAccept;

        // numSuccessfulPolls is the number of times this tx was the successful
        // result of a network poll
        int numSuccessfulPolls;

        // lastVote is the last poll number that this tx was included in a
        // successful network poll. This timestamp is needed to ensure correctness
        // in the case that a tx was rejected when it was preferred in a conflict
        // set and there was a tie for the second highest numSuccessfulPolls.
        int lastVote;

        // tx is the actual transaction this node represents
        final Tx tx;
    }

    private final Map<ID, inputTx>   txs   = new HashMap<>();
    private final Map<ID, inputUTXO> utxos = new HashMap<>();

    public Input(Context ctx, Parameters params) {
        super(ctx, params);
    }

    @Override
    public void accept(ID txID) {
        inputTx txNode = txs.remove(txID);
        // We are accepting the tx, so we should remove the node from the graph.

        // Get the conflicts of this tx so that we can reject them
        Collection<ID> conflicts = conflicts(txNode.tx);

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
        conflicts.forEach(e -> reject(Collections.singletonList(e)));
        acceptTx(txNode.tx);
    }

    @Override
    public void add(Tx tx) {
        if (!shouldVote(this, tx)) {
            return;
        }

        ID txID = tx.id();
        inputTx txNode = new inputTx(tx);

        // This tx should be added to the virtuous sets and preferred sets if this
        // tx is virtuous in all of the UTXOs it is trying to consume.
        boolean virtue = true;

        // For each UTXO consumed by the tx:
        // * Mark this tx as attempting to consume this UTXO
        // * Mark the UTXO as being rogue if applicable
        for (ID inputID : tx.inputIDs()) {
            inputUTXO utxo = utxos.get(inputID);
            if (utxo != null) {
                // If the utxo was already being consumed by another tx, this utxo
                // is now rogue.
                utxo.rogue = true;
                // Since this utxo is rogue, this tx is rogue as well.
                virtue = false;
                // If this utxo was previously virtuous, then there may be txs that
                // were considered virtuous that are now known to be rogue. If
                // that's the case we should remove those txs from the virtuous
                // sets.
                for (ID conflictID : utxo.spenders) {
                    virtuous.remove(conflictID);
                    virtuousVoting.remove(conflictID);
                }
            } else {
                // If there isn't a conflict for this UTXO, I'm the preferred
                // spender.
                utxo.preference = txID;
            }

            // This UTXO needs to track that it is being spent by this tx.
            utxo.spenders.add(txID);

            // We need to write back
            utxos.put(inputID, utxo);
        }

        if (virtue) {
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
        Set<ID> conflicts = new HashSet<>();
        // The conflicting txs are the union of all the txs that spend an input that
        // this tx spends.
        for (ID utxoID : tx.inputIDs()) {
            inputUTXO utxo = utxos.get(utxoID);
            if (utxo != null) {
                conflicts.addAll(utxo.spenders);
            }
        }
        // A tx can't conflict with itself, so we should make sure to remove the
        // provided tx from the conflict set. This is needed in case this tx is
        // currently processing.
        conflicts.remove(tx.id());
        return conflicts;
    }

    @Override
    public boolean issued(Tx tx) {
        // If the tx is either Accepted or Rejected, then it must have been issued
        // previously.
        if (tx.status().decided()) {
            return true;
        }

        // If the tx is currently processing, then it must have been issued.
        return txs.containsKey(tx.id());
    }

    @Override
    public Set<ID> preferences() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean recordPoll(Bag votes) {
        // Increase the vote ID. This is only updated here and is used to reset the
        // confidence values of transactions lazily.
        currentVote++;

        // This flag tracks if the Avalanche instance needs to recompute its
        // frontiers. Frontiers only need to be recalculated if preferences change
        // or if a tx was accepted.
        boolean changed = false;

        // We only want to iterate over txs that received alpha votes
        votes.setThreshold(parameters.alpha);
        // Get the set of IDs that meet this alpha threshold
        Set<ID> metThreshold = votes.threshold();
        for (ID txID : metThreshold) {

            // Get the node this tx represents
            inputTx txNode = txs.get(txID);
            if (txNode == null) {
                // This tx may have already been accepted because of tx
                // dependencies. If this is the case, we can just drop the vote.
                continue;
            }

            txNode.numSuccessfulPolls++;
            txNode.lastVote = currentVote;

            // This tx is preferred if it is preferred in all of its conflict sets
            boolean preferred = true;
            // This tx is rogue if any of its conflict sets are rogue
            boolean rogue = false;
            // The confidence of the tx is the minimum confidence of all the input's
            // conflict sets
            int confidence = Integer.MAX_VALUE;
            for (ID inputID : txNode.tx.inputIDs()) {
                inputUTXO utxo = utxos.get(inputID);

                // If this tx wasn't voted for during the last poll, the confidence
                // should have been reset during the last poll. So, we reset it now.
                // Additionally, if a different tx was voted for in the last poll,
                // the confidence should also be reset.
                if (utxo.lastVote + 1 != currentVote || !txID.equals(utxo.color)) {
                    utxo.confidence = 0;
                }
                utxo.lastVote = currentVote;

                // Update the Snowflake counter and preference.
                utxo.color = txID;
                utxo.confidence++;

                // Update the Snowball preference.
                if (txNode.numSuccessfulPolls > utxo.numSuccessfulPolls) {
                    // If this node didn't previous prefer this tx, then we need to
                    // update the preferences.
                    if (!txID.equals(utxo.preference)) {
                        // If the previous preference lost it's preference in this
                        // input, it can't be preferred in all the inputs.
                        if (preferences.contains(utxo.preference)) {
                            preferences.remove(utxo.preference);
                            // Because there was a change in preferences, Avalanche
                            // will need to recompute its frontiers.
                            changed = true;
                        }
                        utxo.preference = txID;
                    }
                    utxo.numSuccessfulPolls = txNode.numSuccessfulPolls;
                } else {
                    // This isn't the preferred choice in this conflict set so this
                    // tx isn't be preferred.
                    preferred = false;
                }

                // If this utxo is rogue, the transaction must have at least one
                // conflict.
                rogue = rogue || utxo.rogue;

                // The confidence of this tx is the minimum confidence of its
                // inputs.
                if (confidence > utxo.confidence) {
                    confidence = utxo.confidence;
                }

                // The input isn't a pointer, so it must be written back.
                utxos.put(inputID, utxo);
            }

            // If this tx is preferred and it isn't already marked as such, mark the
            // tx as preferred and for Avalanche to recompute the frontiers.
            if (preferred && !preferences.contains(txID)) {
                preferences.add(txID);
                changed = true;
            }

            // If the tx should be accepted, then we should defer its acceptance
            // until its dependencies are decided. If this tx was already marked to
            // be accepted, we shouldn't register it again.
            if (!txNode.pendingAccept
                    && ((!rogue && confidence >= parameters.betaVirtuous) || confidence >= parameters.betaRogue)) {
                // Mark that this tx is pending acceptance so acceptance is only
                // registered once.
                txNode.pendingAccept = true;

                registerAcceptor(this, txNode.tx);
                if (!errs.isEmpty()) {
                    return changed;
                }
            }

            if (txNode.tx.status() == Status.ACCEPTED) {
                // By accepting a tx, the state of this instance has changed.
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public void reject(Collection<ID> rejected) {
        for (ID conflictID : rejected) {
            // We are rejecting the tx, so we should remove it from the graph
            inputTx conflict = txs.remove(conflictID);

            // While it's statistically unlikely that something being rejected is
            // preferred, it is handled for completion.
            preferences.remove(conflictID);

            // Remove this tx from all the conflict sets it's currently in
            removeConflict(conflictID, conflict.tx.inputIDs());

            rejectTx(conflict.tx);
        }
    }

    public void removeConflict(ID txID, Collection<ID> inputIDs) {
        for (ID inputID : inputIDs) {
            inputUTXO utxo = utxos.get(inputID);
            if (utxo == null) {
                // if the utxo doesn't exists, it was already consumed, so there is
                // no mapping left to update.
                continue;
            }

            // This tx is no longer attempting to spend this utxo.
            utxo.spenders.remove(txID);

            // If there is nothing attempting to consume the utxo anymore, remove it
            // from memory.
            if (utxo.spenders.isEmpty()) {
                utxos.remove(inputID);
                continue;
            }

            // If I'm rejecting the non-preference, there is nothing else to update.
            if (!utxo.preference.equals(txID)) {
                utxos.put(inputID, utxo);
                continue;
            }

            // If I was previously preferred, I must find who should now be
            // preferred.
            ID preference = ID.ORIGIN;
            int numSuccessfulPolls = -1;
            int lastVote = 0;

            // Find the new Snowball preference
            inputTx txNode;
            for (ID spender : utxo.spenders) {
                txNode = txs.get(spender);
                if (txNode.numSuccessfulPolls > numSuccessfulPolls
                        || (txNode.numSuccessfulPolls == numSuccessfulPolls && lastVote < txNode.lastVote)) {
                    preference = spender;
                    numSuccessfulPolls = txNode.numSuccessfulPolls;
                    lastVote = txNode.lastVote;
                }
            }

            // Update the preferences
            utxo.preference = preference;
            utxo.numSuccessfulPolls = numSuccessfulPolls;

            utxos.put(inputID, utxo);

            // We need to check if this tx is now preferred
            txNode = txs.get(preference);
            boolean isPreferred = true;
            for (ID id : txNode.tx.inputIDs()) {
                inputUTXO input = utxos.get(id);

                if (!preference.equals(input.preference)) {
                    // If this preference isn't the preferred color, the tx isn't
                    // preferred. Also note that the input might not exist, in which
                    // case this tx is going to be rejected in a later iteration.
                    isPreferred = false;
                    break;
                }
            }
            if (isPreferred) {
                // If I'm preferred in all my conflict sets, I'm preferred.
                preferences.add(preference);
            }
        }
    }

    public boolean isVirtuous(Tx tx) {
        ID txID = tx.id();
        for (ID utxoID : tx.inputIDs()) {
            inputUTXO utxo = utxos.get(utxoID);
            // If the UTXO wasn't currently processing, then this tx won't conflict
            // due to this UTXO.
            if (utxo == null) {
                continue;
            }
            // If this UTXO is rogue, then this tx will have at least one conflict.
            if (utxo.rogue) {
                return false;
            }
            // This UTXO is currently virtuous, so it must be spent by only one tx.
            // If that tx is different from this tx, then these txs would conflict.
            if (!utxo.spenders.contains(txID)) {
                return false;
            }
        }

        // None of the UTXOs consumed by this tx imply that this tx would be rogue,
        // so it is virtuous as far as this consensus instance knows.
        return true;
    }

}
