/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.Committee.validatorsOf;
import static com.salesforce.apollo.choam.support.HashedBlock.hash;

import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesfoce.apollo.choam.proto.Genesis;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesforce.apollo.choam.Committee.CommitteeCommon;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedBlock.NullBlock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.utils.SimpleChannel;

/**
 * Combine Honnete Ober Advancer Mercantiles.
 * 
 * @author hal.hildebrand
 *
 */
public class CHOAM { 
    private class Associate extends CommitteeCommon {

        public Associate(HashedBlock block, Context<Member> context) {
            super(block, context);
            // TODO Auto-generated constructor stub
        }

        @Override
        public void accept(HashedBlock next) {
            // TODO Auto-generated method stub
        }

        @Override
        public Parameters params() {
            return params;
        }

    }
 
    private class Client extends CommitteeCommon {

        public Client(HashedBlock block, Context<Member> context) {
            super(block, context);
            // TODO Auto-generated constructor stub
        }

        @Override
        public void accept(HashedBlock next) {
            // TODO Auto-generated method stub
        }

        @Override
        public Parameters params() {
            return params;
        }

    }

    private class Formation implements Committee {

        @Override
        public void accept(HashedBlock next) {
            Genesis genesis = next.block.getGenesis();
            formCommittee(next);
            process(genesis.getInitializeList());
        }

        @Override
        public long getHeight() {
            return 0;
        }

        @Override
        public Digest getId() {
            return params.genesisViewId();
        }

        @Override
        public boolean validate(HashedBlock hb) {
            var block = hb.block;
            if (!block.hasGenesis()) {
                log.debug("Invalid genesis block: {} on: {}", hb.hash, params.member());
                return false;
            }
            var validators = validatorsOf(block.getGenesis().getInitialView(), params.context());
            var headerHash = hash(hb.block.getHeader(), params.digestAlgorithm()).getBytes();
            return hb.block.getCertificationsList().stream().filter(c -> validate(headerHash, c, validators))
                           .count() > params.context().toleranceLevel() + 1;
        }

        @Override
        public Parameters params() {
            return params;
        }
    }

    private final ReliableBroadcaster        combine;
    private Committee                        current;
    private HashedBlock                      head;
    private final SimpleChannel<HashedBlock> linear;
    private final Logger                     log     = LoggerFactory.getLogger(CHOAM.class);
    private final Parameters                 params;
    private final PriorityQueue<HashedBlock> pending = new PriorityQueue<>();
    private final AtomicBoolean              started = new AtomicBoolean();

    public CHOAM(Parameters params) {
        this.params = params;
        combine = new ReliableBroadcaster(params.combineParameters(), params.communications());
        combine.registerHandler((ctx, messages) -> combine(messages));
        linear = new SimpleChannel<>(100);
        head = new NullBlock(params.digestAlgorithm());
        current = new Formation();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        linear.consumeEach(b -> accept(b));
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        linear.close();
    }

    private void accept(HashedBlock next) {
        head = next;
        current.accept(next);
    }

    private void combine() {
        var next = pending.peek();
        while (isNext(next)) {
            if (current.validate(next)) {
                HashedBlock nextBlock = pending.poll();
                if (nextBlock == null) {
                    return;
                }
                linear.submit(nextBlock);
            } else {
                log.debug("unable to validate block: {} on: {}", next.hash, params.member());
            }
            next = pending.peek();
        }
    }

    private void combine(List<Msg> messages) {
        messages.forEach(m -> combine(m));
        combine();
    }

    private void combine(Msg m) {
        Block block;
        try {
            block = Block.parseFrom(m.content());
        } catch (InvalidProtocolBufferException e) {
            log.debug("unable to parse block content from {} on: {}", m.source(), params.member());
            return;
        }
        pending.add(new HashedBlock(params.digestAlgorithm(), block));
    }

    private void formCommittee(HashedBlock hb) {
        Reconfigure reconfigure = hb.block.getReconfigure();
        Digest id = new Digest(reconfigure.getId());
        var proposed = params.context().successors(id);
        current = proposed.contains(params.member()) ? new Associate(head, params.context())
                                                     : new Client(hb, params.context());
    }

    private boolean isNext(HashedBlock next) {
        return next != null && next.height() == head.height() + 1 && head.hash.equals(next.getPrevious());
    }

    private void process(List<ExecutedTransaction> executions) {
        // TODO Auto-generated method stub
    }
}
