/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.Committee.validatorsOf;
import static com.salesforce.apollo.choam.support.HashedBlock.buildHeader;

import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock.NullBlock;
import com.salesforce.apollo.choam.support.Store;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.utils.Channel;
import com.salesforce.apollo.utils.SimpleChannel;

/**
 * Combine Honnete Ober Advancer Mercantiles.
 * 
 * @author hal.hildebrand
 *
 */
public class CHOAM {

    /** a member of the current committee */
    class Associate extends Administration { 
        private final Producer       producer;
        private HashedCertifiedBlock viewChange;

        Associate(HashedCertifiedBlock block, Map<Member, Verifier> validators) {
            super(validators);
            this.viewChange = block;
            var reconfigure = viewChange.block.getReconfigure();
            Digest id = new Digest(reconfigure.getId());
            var context = new Context<>(id, 0.33, params.context().getRingCount());
            params.context().successors(id).forEach(m -> {
                if (validators.containsKey(m)) {
                    context.activate(m);
                } else {
                    context.offline(m);
                }
            });
            producer = new Producer(this,
                                    new ReliableBroadcaster(params.coordination().clone().setMember(params.member())
                                                                  .setContext(context).build(),
                                                            params.communications()));
        }

        @Override
        public void complete() {
            producer.complete();
        }

        @Override
        public HashedBlock getViewChange() {
            assert viewChange != null;
            return viewChange;
        }

    }

    /** abstract class to maintain the common state */
    private abstract class Administration implements Committee {
        private final Map<Member, Verifier> validators;

        public Administration(Map<Member, Verifier> validator) {
            this.validators = validator;
        }

        @Override
        public void accept(HashedCertifiedBlock hb) {
            head = hb;
            process();
        }

        @Override
        public void complete() {
        }

        @Override
        public Parameters params() {
            return params;
        }

        @Override
        public boolean validate(HashedCertifiedBlock hb) {
            return validate(hb, validators);
        }
    }

    /** a client of the current committee */
    private class Client extends Administration {
        protected final HashedBlock viewChange;

        public Client(HashedBlock viewChange, Map<Member, Verifier> validators) {
            super(validators);
            this.viewChange = viewChange;

        }

        @Override
        public HashedBlock getViewChange() {
            return viewChange;
        }
    }

    /** The Genesis formation comittee */
    private class Formation implements Committee {

        @Override
        public void accept(HashedCertifiedBlock hb) {
            head = hb;
            genesis = head;
            checkpoint = head;
            view = head;
            process();
        }

        @Override
        public void complete() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public HashedBlock getViewChange() {
            assert genesis != null;
            return genesis;
        }

        @Override
        public Parameters params() {
            return params;
        }

        @Override
        public boolean validate(HashedCertifiedBlock hb) {
            var block = hb.block;
            if (!block.hasGenesis()) {
                log.debug("Invalid genesis block: {} on: {}", hb.hash, params.member());
                return false;
            }
            return validateRegeneration(hb);
        }
    }

    public static Block reconfigure(Digest id, Map<Member, Join> joins, HashedBlock head, Context<Member> context,
                                    HashedBlock lastViewChange, Parameters params) {
        var builder = Reconfigure.newBuilder().setCheckpointBlocks(params.checkpointBlockSize()).setId(id.toDigeste())
                                 .setEpochLength(params.ethereal().getEpochLength())
                                 .setNumberOfEpochs(params.ethereal().getNumberOfEpochs());

        // Canonical labeling of the view members for Ethereal
        var ring0 = context.ring(0);
        var remapped = joins.keySet().stream().collect(Collectors.toMap(m -> ring0.hash(m), m -> m));
        remapped.keySet().stream().sorted().map(d -> remapped.get(d)).peek(m -> builder.addJoins(joins.get(m)))
                .forEach(m -> builder.addView(joins.get(m).getMember()));

        var reconfigure = builder.build();
        return Block.newBuilder()
                    .setHeader(buildHeader(params.digestAlgorithm(), reconfigure, head.hash, head.height() + 1,
                                           lastViewChange.height(), lastViewChange.hash,
                                           lastViewChange.block.getHeader().getLastReconfig(),
                                           new Digest(lastViewChange.block.getHeader().getLastReconfigHash())))
                    .setReconfigure(reconfigure).build();
    }

    @SuppressWarnings("unused")
    private HashedCertifiedBlock                      checkpoint;
    private final ReliableBroadcaster                 combine;
    private Committee                                 current;
    private HashedCertifiedBlock                      genesis;
    private HashedCertifiedBlock                      head;
    private final Channel<HashedCertifiedBlock>       linear;
    private final Logger                              log     = LoggerFactory.getLogger(CHOAM.class);
    private final Parameters                          params;
    private final PriorityQueue<HashedCertifiedBlock> pending = new PriorityQueue<>();
    private final AtomicBoolean                       started = new AtomicBoolean();
    private final Store                               store;
    @SuppressWarnings("unused")
    private HashedCertifiedBlock                      view;

    public CHOAM(Parameters params, Store store) {
        this.store = store;
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
        linear.open();
        linear.consumeEach(b -> accept(b));
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        linear.close();
    }

    private void accept(HashedCertifiedBlock next) {
        head = next;
        store.put(next);
        current.accept(next);
    }

    private void checkpoint() {
        // TODO Auto-generated method stub

    }

    private void combine() {
        var next = pending.peek();
        while (isNext(next)) {
            if (current.validate(next)) {
                HashedCertifiedBlock nextBlock = pending.poll();
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
        CertifiedBlock block;
        try {
            block = CertifiedBlock.parseFrom(m.content());
        } catch (InvalidProtocolBufferException e) {
            log.debug("unable to parse block content from {} on: {}", m.source(), params.member());
            return;
        }
        pending.add(new HashedCertifiedBlock(params.digestAlgorithm(), block));
    }

    private void execute(ExecutedTransaction execution) {
        params.executor().execute(head.hash, execution, (r, t) -> {
        });
    }

    private boolean isNext(HashedBlock next) {
        return next != null && next.height() == head.height() + 1 && head.hash.equals(next.getPrevious());
    }

    private void process() {
        switch (head.block.getBodyCase()) {
        case CHECKPOINT:
            checkpoint();
            break;
        case EXECUTIONS:
            head.block.getExecutions().getExecutionsList().forEach(et -> execute(et));
            break;
        case RECONFIGURE:
            reconfigure(head.block.getReconfigure());
            break;
        case GENESIS:
            reconfigure(head.block.getGenesis().getInitialView());
            head.block.getGenesis().getInitializeList().forEach(et -> execute(et));
            break;
        default:
            break;
        }
    }

    private void reconfigure(Reconfigure reconfigure) {
        current.complete();
        var validators = validatorsOf(reconfigure, params.context());
        if (validators.containsKey(params.member())) {
            current = new Associate(head, validators);
        } else {
            current = new Client(head, validators);
        }
    }
}
