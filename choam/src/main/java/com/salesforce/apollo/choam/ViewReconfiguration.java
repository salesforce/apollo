/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Validate;
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

    private final boolean                forGenesis;
    private final HashedBlock            previous;
    private final CertifiedBlock.Builder reconfiguration = CertifiedBlock.newBuilder();
    private volatile Digest              reconfigurationHash;
    private final BlockProducer          reconfigureBlock;
    private volatile Validate            validation;

    public ViewReconfiguration(Digest nextViewId, ViewContext vc, HashedBlock previous,
                               CommonCommunications<Terminal, ?> comms, BlockProducer reconfigureBlock,
                               boolean forGenesis) {
        super(nextViewId, vc, comms);
        this.reconfigureBlock = reconfigureBlock;
        this.previous = previous;
        this.forGenesis = forGenesis;
    }

    @Override
    public void continueValidating() {
        log.debug("Assembly: {} reconfigured, broadcasting validations on: {}", nextViewId, params().member());
        AtomicInteger countDown = new AtomicInteger(3);
        coordinator.register(round -> {
            if (round % coordinator.getContext().timeToLive() == 0) {
                if (countDown.decrementAndGet() > 0) {
                    coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
                } else {
                    transitions.complete();
                }
            }
        });
    }

    public void start() {
        transitions.fsm().enterStartState();
    }

    @Override
    public void validation(Validate validate) {
        if (!reconfiguration.hasBlock()) {
            log.trace("No block for validation on: {}", params().member());
            return;
        }
        final Digest validateHash = new Digest(validate.getHash());
        final Digest current = reconfigurationHash;
        if (current != null && current.equals(validateHash)) {
            reconfiguration.addCertifications(validate.getWitness());
            log.trace("Adding validation on: {}", params().member());
            maybePublish();
        } else {
            log.trace("Invalid validation: {} expected: {} on: {}", validateHash, current, params().member());
        }
    }

    @Override
    protected void assembled(Map<Member, Join> aggregate) {
        final int toleranceLevel = params().toleranceLevel();
        if (aggregate.size() > toleranceLevel) {
            var reconfigure = forGenesis ? reconfigureBlock.genesis(aggregate, nextViewId, previous)
                                         : reconfigureBlock.reconfigure(aggregate, nextViewId, previous);
            reconfiguration.setBlock(reconfigure);
            var rhb = new HashedBlock(params().digestAlgorithm(), reconfigure);
            reconfigurationHash = rhb.hash;
            validation = view.generateValidation(rhb);
            coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
            reconfiguration.addCertifications(validation.getWitness());
            log.debug("Aggregate of: {} threshold reached: {} block: {} on: {}", nextViewId, aggregate.size(),
                      reconfigurationHash, params().member());
            transitions.nominated();
            maybePublish();
        } else {
            log.debug("Aggregate of: {} threshold failed: {} on: {}", nextViewId, aggregate.size(), params().member());
            transitions.failed();
        }
    }

    private void maybePublish() {
        if (reconfiguration.hasBlock() && reconfiguration.getCertificationsCount() > params().toleranceLevel()) {
            final HashedCertifiedBlock block = new HashedCertifiedBlock(params().digestAlgorithm(),
                                                                        reconfiguration.build());
            log.trace("Publishing reconfiguration: {} on: {}", block.hash, params().member());
            view.publish(block);
            transitions.reconfigured();
        }
    }

    protected boolean process(Digest sender, Coordinate coordination) {
        if (!super.process(sender, coordination)) {
            if (coordination.hasValidate()) {
                transitions.validate(coordination.getValidate());
            }
        }
        return true;
    }

}
