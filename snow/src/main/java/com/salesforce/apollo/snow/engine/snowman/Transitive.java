/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.snowman;

import java.util.List;
import java.util.Set;

import org.eclipse.jetty.util.SharedBlockingCallback.Blocker;

import com.salesforce.apollo.snow.consensus.snowman.Consensus;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class Transitive {
    public final Parameters       params;
    public final Consensus        consensus;
    private final Set<Poll>       polls;
    private final Requests        blkReqs;
    private final Set<ID>         pending;
    private final Blocker         blocked;
    private final List<Throwable> errors;

    public Transitive(Parameters params, Consensus consensus, Set<Poll> polls, Requests blkReqs, Set<ID> pending,
            Blocker blocked, List<Throwable> errors) {
        this.params = params;
        this.consensus = consensus;
        this.polls = polls;
        this.blkReqs = blkReqs;
        this.pending = pending;
        this.blocked = blocked;
        this.errors = errors;
    }
}
