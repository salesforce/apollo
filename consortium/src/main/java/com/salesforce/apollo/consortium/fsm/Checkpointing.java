/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import java.util.List;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.salesfoce.apollo.consortium.proto.CheckpointProcessing;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.CollaboratorContext;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.support.EnqueuedTransaction;
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
        @Exit
        public void cancelTimer() {
            context().cancel(Timers.CHECKPOINT_TIMEOUT);
        }

        @Override
        public Transitions checkpointGenerated() {
            return CollaboratorFsm.FOLLOWER;
        }

        @Override
        public Transitions checkpointTimeout() {
            return CollaboratorFsm.FOLLOWER;
        }

        @Override
        public Transitions deliverCheckpointing(CheckpointProcessing checkpointProcessing, Member from) {
            context().deliverCheckpointing(checkpointProcessing, from);
            return null;
        }
    },
    LEADER {
        @Override
        public Transitions checkpointGenerated() {
            return CollaboratorFsm.LEADER;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            CollaboratorContext context = context();
            context.deliverValidate(validation);
            context.totalOrderDeliver();
            return null;
        }

        @Entry
        public void generateCheckpointBlock() {
            context().generateCheckpointBlock();
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
