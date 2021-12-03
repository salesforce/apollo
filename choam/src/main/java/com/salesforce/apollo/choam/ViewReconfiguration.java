/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Reassemble;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.Validations;
import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class ViewReconfiguration extends ViewAssembly {
    private final static Logger log = LoggerFactory.getLogger(ViewReconfiguration.class);

    private final AtomicBoolean         feed      = new AtomicBoolean();
    private final boolean               forGenesis;
    private final HashedBlock           previous;
    private final AtomicBoolean         published = new AtomicBoolean();
    private volatile HashedBlock        reconfiguration;
    private final BlockProducer         reconfigureBlock;
    private final Map<Member, Validate> witnesses = new ConcurrentHashMap<>();

    public ViewReconfiguration(Digest nextViewId, ViewContext vc, HashedBlock previous,
                               CommonCommunications<Terminal, ?> comms, BlockProducer reconfigureBlock,
                               boolean forGenesis) {
        super(nextViewId, vc, comms);
        this.reconfigureBlock = reconfigureBlock;
        this.previous = previous;
        this.forGenesis = forGenesis;
    }

    @Override
    public void certifyBlock() {

    }

    @Override
    public void complete() {
        super.complete();
        generate();
        feed.set(true);
        transitions.reconfigureBlock();
    }

    @Override
    public void produceBlock() {
        if (published.compareAndSet(false, true)) {
            if (witnesses.size() > params().toleranceLevel()) {
                publish();
            } else {
                log.error("Failed reconfiguration block: {} witnesses: {} required: {} on: {}", reconfiguration.hash,
                          witnesses.size(), params().toleranceLevel() + 1, params().member());
                return;
            }
        }
        log.info("Reconfiguration block: {} produced on: {}", reconfiguration.hash, params().member());
    }

    @Override
    protected void validate(Validate v) {
        if (!feed.get()) {
            super.validate(v);
            return;
        }
        log.trace("Validating block: {} produced on: {}", reconfiguration.hash, params().member());
        if (!view.validate(reconfiguration, v)) {
            return;
        }
        witnesses.put(view.context().getMember(Digest.from(v.getWitness().getId())), v);
        if (witnesses.size() > params().toleranceLevel()) {
            if (published.compareAndSet(false, true)) {
                publish();
            }
        }
    }

    private void generate() {
        final var slate = getSlate();
        reconfiguration = new HashedBlock(params().digestAlgorithm(),
                                          forGenesis ? reconfigureBlock.genesis(slate, nextViewId, previous)
                                                     : reconfigureBlock.reconfigure(slate, nextViewId, previous));
        var validate = view.generateValidation(reconfiguration);
        log.trace("Certifying reconfiguration block: {} for: {} count: {} on: {}", reconfiguration.hash, nextViewId,
                  slate.size(), params().member());
        ds.clear();
        try {
            assert ds.size() == 0 : "Existing data! size: " + ds.size();
            ds.put(Reassemble.newBuilder().setValidations(Validations.newBuilder().addValidations(validate).build())
                             .build().toByteString());
            for (int i = 0; i < 20; i++) {
                ds.put(ByteString.EMPTY);
            }
        } catch (InterruptedException e) {
            log.error("Failed enqueing block reconfiguration validation for: {} on: {}", nextViewId, params().member(),
                      e);
            transitions.failed();
        }
    }

    @Override
    protected int epochs() {
        return 4;
    }

    private void publish() {
        var b = CertifiedBlock.newBuilder().setBlock(reconfiguration.block);
        witnesses.entrySet().stream().sorted(Comparator.comparing(e -> e.getKey().getId())).map(e -> e.getValue())
                 .forEach(v -> b.addCertifications(v.getWitness()));
        view.publish(new HashedCertifiedBlock(params().digestAlgorithm(), b.build()));
        log.debug("{}Reconfiguration block: {} published for: {} on: {}", forGenesis ? "Genesis " : "",
                  reconfiguration.hash, nextViewId, params().member());
    }
}
