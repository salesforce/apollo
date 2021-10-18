/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

/**
 * Abstract utility implementation for gossipers
 * 
 * @author hal.hildebrand
 *
 */
public class Gossiper {
    private static final Logger log = LoggerFactory.getLogger(Gossiper.class);

    private final Orderer orderer;

    public Gossiper(Orderer orderer) {
        this.orderer = orderer;
    }

    public Gossip gossip() {
        return gossip(DigestAlgorithm.DEFAULT.getOrigin());
    }

    public Gossip gossip(Digest context) {
        log.trace("Gossiping for: {} on: {}", context, orderer.getConfig().pid());
        return Gossip.newBuilder().setContext(context.toDigeste()).setHave(orderer.have().toBff()).build();
    }

    public Update gossip(Gossip gossip) {
        Update update = orderer.missing(BloomFilter.from(gossip.getHave()));
        log.trace("Gossip received for: {} missing: {} on: {}", Digest.from(gossip.getContext()),
                 update.getMissingCount(), orderer.getConfig().pid());
        return update;
    }

    public void update(Update update) { 
        List<PreUnit> missing = update.getMissingList().stream()
                                      .map(pus -> PreUnit.from(pus, orderer.getConfig().digestAlgorithm()))
//                                      .filter(pu -> pu.verify(config.verifiers()))
                                      .toList();
        if (missing.isEmpty()) {
            return;
        }
        log.trace("Gossip update: {} on: {}", missing.size(), orderer.getConfig().pid());
        orderer.addPreunits(PreUnit.topologicalSort(new ArrayList<>(missing)));
    }
}
