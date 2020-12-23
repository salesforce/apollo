/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import java.util.List;

import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesforce.apollo.consortium.EnqueuedTransaction;
import com.salesforce.apollo.membership.Member;

/**
 * 
 * FSM for generating checkpoints
 * 
 * @author hal.hildebrand
 *
 */
public enum Checkpointing implements Transitions {
    FOLLOWER {
        @Override
        public Transitions deliverCheckpoint(Checkpoint checkpoint, Member from) {
            context().deliverCheckpoint(checkpoint, from);
            return null;
        }
    },
    LEADER {

        @Override
        public Transitions checkpoint() {
            context().generateCheckpointBlock();
            return null;
        }

    };

    @Override
    public Transitions deliverStop(Stop stop, Member from) {
        return null;
    }

    @Override
    public Transitions deliverStopData(StopData stopData, Member from) {
        return null;
    }

    @Override
    public Transitions deliverSync(Sync sync, Member from) {
        return null;
    }

    @Override
    public Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
        context().reschedule(transactions);
        return null;
    }

}
