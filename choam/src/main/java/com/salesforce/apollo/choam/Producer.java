/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import com.google.protobuf.Any;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.CHOAM.Associate;
import com.salesforce.apollo.ethereal.Data.PreBlock;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class Producer implements DataSource {
    private record broadcast(short source, List<PreUnit> pus) {}

    private final Associate                  associate;
    private final Controller                 controller;
    private final ReliableBroadcaster        coordinator;
    private final Ethereal                   ethereal;
    private final CertifiedBlock.Builder     reconfiguration = CertifiedBlock.newBuilder();
    private final BlockingDeque<Transaction> transactions    = new LinkedBlockingDeque<>();

    public Producer(Associate associate, Block viewChange, ReliableBroadcaster coordinator) {
        ethereal = new Ethereal();
        this.associate = associate;
        if (viewChange != null) {
            reconfiguration.setBlock(viewChange);
        }
        this.coordinator = coordinator;
        this.coordinator.registerHandler((ctx, msgs) -> msgs.forEach(msg -> process(msg)));
        controller = ethereal.deterministic(associate.params().ethereal().clone().build(), this,
                                            preblock -> preblock(preblock), preUnit -> broadcast(preUnit));
    }

    @Override
    public Any getData() {
        // TODO Auto-generated method stub
        return null;
    }

    private void broadcast(PreUnit preUnit) {
        // TODO Auto-generated method stub
    }

    private void preblock(PreBlock preblock) {
        // TODO Auto-generated method stub
    }

    private void process(Msg msg) {
        // TODO Auto-generated method stub
    }
}
