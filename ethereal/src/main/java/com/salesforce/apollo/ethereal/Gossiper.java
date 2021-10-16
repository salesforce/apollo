/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.List;

import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

/**
 * @author hal.hildebrand
 *
 */
abstract public class Gossiper {
    public interface GossipClient {
        Update gossip(Gossip gossip);
    }

    private final Orderer orderer;

    public Gossiper(Orderer orderer) {
        this.orderer = orderer;
    }

    public Update gossip(Gossip gossip) {
        return Update.newBuilder().addAllMissing(orderer.missing(BloomFilter.from(gossip.getHave()))).build();
    }

    public void update(Update update) {
        final var config = orderer.getConfig();
        List<PreUnit> missing = update.getMissingList().stream()
                                      .map(pus -> PreUnit.from(pus, orderer.getConfig().digestAlgorithm()))
                                      .filter(pu -> pu.verify(config.verifiers())).toList();
        if (missing.isEmpty()) {
            return;
        }
        orderer.addPreunits(PreUnit.topologicalSort(missing));
    }

    abstract protected Digest getContext();

    abstract protected GossipClient linkFor(short pid);
}
