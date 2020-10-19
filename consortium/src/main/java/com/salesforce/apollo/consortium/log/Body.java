/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.log;

import java.util.List;

/**
 * @author hal.hildebrand
 *
 */
public class Body {
    private final long              consensusId;
    private final List<Proof>       proofs;
    private final List<Response>    responses;
    private final List<Transaction> transactions;

    public Body(long consensusId, List<Transaction> transactions, List<Proof> proofs, List<Response> responses) {
        this.consensusId = consensusId;
        this.transactions = transactions;
        this.proofs = proofs;
        this.responses = responses;
    }

    public long getConsensusId() {
        return consensusId;
    }

    public List<Proof> getProofs() {
        return proofs;
    }

    public List<Response> getResponses() {
        return responses;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }
}
