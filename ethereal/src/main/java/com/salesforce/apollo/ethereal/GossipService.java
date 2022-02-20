/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * Abstract utility implementation for gossipers
 * 
 * @author hal.hildebrand
 *
 */
public class GossipService {
    private static final Logger log = LoggerFactory.getLogger(GossipService.class);

    private final List<BloomFilter<Digest>> biffs;
    private final Lock                      mx = new ReentrantLock();
    private final Orderer                   orderer;

    public GossipService(Controller controller) {
        this(controller.orderer());
    }

    public GossipService(Orderer orderer) {
        this.orderer = orderer;
        biffs = new ArrayList<>();
        Config config = orderer.getConfig();
        int count = Math.max(4, config.nProc());
        for (int i = 0; i < count; i++) {
            biffs.add(new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(),
                                            config.epochLength() * config.numberOfEpochs() * config.nProc() * 2,
                                            config.fpr()));
        }
    }

    public Gossip gossip(Digest context, int ring) {
        log.trace("Gossiping for: {} on: {}", context, orderer.getConfig().pid());
        mx.lock();
        try {
            return Gossip.newBuilder()
                         .setContext(context.toDigeste())
                         .setRing(ring)
                         .setHave(biffs.get(Utils.bitStreamEntropy().nextInt(biffs.size())).toBff())
                         .build();
        } finally {
            mx.unlock();
        }
    }

    public Update gossip(Gossip gossip) {
        Update.Builder update = orderer.missing(BloomFilter.from(gossip.getHave()));
        log.trace("GossipService received for: {} missing: {} on: {}", Digest.from(gossip.getContext()),
                  update.getMissingCount(), orderer.getConfig().pid());
        return update.setHave(biffs.get(Utils.bitStreamEntropy().nextInt(biffs.size())).toBff()).build();
    }

    public void update(Update update) {
        List<PreUnit> missing = update.getMissingList()
                                      .stream()
                                      .map(pus -> PreUnit.from(pus, orderer.getConfig().digestAlgorithm()))
//                                      .filter(pu -> pu.verify(config.verifiers()))
                                      .toList();
        if (missing.isEmpty()) {
            return;
        }
        log.trace("GossipService update: {} on: {}", missing.size(), orderer.getConfig().pid());
        mx.lock();
        try {
            missing.forEach(pu -> {
                biffs.forEach(biff -> biff.add(pu.hash()));
            });
        } finally {
            mx.unlock();
        }
        orderer.addPreunits(PreUnit.topologicalSort(new ArrayList<>(missing)));
    }

    public Update updateFor(BloomFilter<Digest> have) {
        Update.Builder update = orderer.missing(have);
        return update.setHave(biffs.get(Utils.bitStreamEntropy().nextInt(biffs.size())).toBff()).build();
    }
}
