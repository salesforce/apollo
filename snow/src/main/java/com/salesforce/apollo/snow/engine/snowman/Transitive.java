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
    public final Parameters params;
    public final Consensus consensus;
    private final Set<Poll> polls;
    private Requests blkReqs;
    private Set<ID> pending;
    private Blocker blocked;
    private List<Throwable> errors;
}
