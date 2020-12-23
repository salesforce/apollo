/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.io.File;

import com.salesfoce.apollo.consortium.proto.Checkpoint;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointState {
    public final Checkpoint checkpoint;
    public final File       state;

    public CheckpointState(Checkpoint checkpoint, File state) {
        this.checkpoint = checkpoint;
        this.state = state;
    }
}
