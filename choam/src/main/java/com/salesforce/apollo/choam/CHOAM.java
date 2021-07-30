/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.Committee.validatorsOf;
import static com.salesforce.apollo.choam.support.HashedBlock.buildHeader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Checkpoint;
import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesfoce.apollo.choam.proto.Genesis;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesfoce.apollo.choam.proto.ViewMember;
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
@SuppressWarnings("unused")
public class CHOAM {

    /** A member of the current committee */
    class Associate extends Administration {
        private final Producer       producer;
        private HashedCertifiedBlock viewChange;

        Associate(Digest id, Set<Member> appointed, Map<Member, Verifier> validators, Map<Member, Join> joins) {
            super(validators, head.height() + params.key());
            var context = new Context<>(id, 0.33, params.context().getRingCount());
            params.context().successors(id).forEach(m -> {
                if (appointed.contains(m)) {
                    context.activate(m);
                } else {
                    context.offline(m);
                }
            });
            if (params.member().equals(context.ring(0).get(0))) {
                producer = new Producer(this,
                                        new ReliableBroadcaster(params.coordination().clone().setMember(params.member())
                                                                      .setContext(context).build(),
                                                                params.communications()));
            } else {
                producer = null;
            }
        }

        @Override
        public HashedBlock getViewChange() {
            assert viewChange != null;
            return viewChange;
        }

        void install() {
        }

    }

    /** abstract class to maintain the common state */
    private abstract class Administration implements Committee {
        private final long                  key;
        private final Map<Member, Verifier> validators;

        public Administration(Map<Member, Verifier> validator, long key) {
            this.key = key;
            this.validators = validator;
        }

        @Override
        public void accept(HashedCertifiedBlock hb) {
            if (hb.height() == key) {
                next();
            }
            process();
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

    /** the assembly for the next committee. */
    private class Assembly {
        private final Set<Member>       appointed;
        private final Digest            id;
        private final Map<Member, Join> joins = new HashMap<>();

        Assembly(Digest id) {
            this.id = id;
            appointed = new HashSet<>(params.context().successors(id));
        }

        void install() {
            Map<Member, ViewMember> validators = joins.entrySet().stream()
                                                      .collect(Collectors.toMap(e -> e.getKey(),
                                                                                e -> e.getValue().getMember()));
            if (validators.size() <= params.context().toleranceLevel()) {
                log.error("Unable to install view: {} due to insufficient members joining: {} required: {} on: {}", id,
                          joins.size(), params.context().toleranceLevel() + 1, params.member());
                return;
            }
            if (validators.containsKey(params.member())) {
                current = new Associate(id, appointed, Committee.validators(validators), joins);
                next = null;
            }
        }

        void join(ExecutedTransaction execution) {
            assert execution.getTransation().hasJoin();

            Join join = execution.getTransation().getJoin();
            if (!id.equals(new Digest(join.getView()))) {
                return;
            }
            Member joining = params.context().getMember(new Digest(join.getMember().getId()));
            if (joining == null) {
                return;
            }
            if (!appointed.contains(joining)) {
                return;
            }
            joins.put(joining, join);
        }
    }

    /** we are but a client of the current committee */
    private class Client extends Administration {
        protected final HashedBlock viewChange;

        public Client(HashedBlock viewChange) {
            super(validatorsOf(viewChange.block.getReconfigure(), params.context()),
                  viewChange.height() + viewChange.block.getReconfigure().getKey());
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
            genesis = hb;
            Genesis genesis = hb.block.getGenesis();
            current = new Client(hb);
            next = null;
            process(genesis.getInitializeList());
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
                                 .setKey(params.key()).setEpochLength(params.ethereal().getEpochLength())
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

    private HashedCertifiedBlock                      checkpoint;
    private final ReliableBroadcaster                 combine;
    private Committee                                 current;
    private HashedCertifiedBlock                      genesis;
    private HashedCertifiedBlock                      head;
    private final Channel<HashedCertifiedBlock>       linear;
    private final Logger                              log     = LoggerFactory.getLogger(CHOAM.class);
    private Assembly                                  next;
    private final Parameters                          params;
    private final PriorityQueue<HashedCertifiedBlock> pending = new PriorityQueue<>();
    private final AtomicBoolean                       started = new AtomicBoolean();
    private final Store                               store;

    public CHOAM(Parameters params, Store store) {
        this.store = store;
        this.params = params;
        combine = new ReliableBroadcaster(params.combineParameters(), params.communications());
        combine.registerHandler((ctx, messages) -> combine(messages));
        linear = new SimpleChannel<>(100);
        head = new NullBlock(params.digestAlgorithm());
        current = new Formation();
        next = new Assembly(params.genesisViewId());
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

    private void checkpoint(Checkpoint checkpoint) {
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

    private void next() {
        next = new Assembly(head.hash);
    }

    private void process() {
        if (head.block.hasExecutions()) {
            process(head.block.getExecutions().getExecutionsList());
        } else if (head.block.hasCheckpoint()) {
            checkpoint(head.block.getCheckpoint());
        }
        throw new IllegalStateException("Should have already installed assembly or processed genesis");

    }

    private void process(List<ExecutedTransaction> executions) {
        for (ExecutedTransaction execution : executions) {
            switch (execution.getTransation().getTransactionCase()) {
            case JOIN: {
                if (next != null) {
                    next.join(execution);
                }
                break;
            }
            case USER:
                execute(execution);
                break;
            default:
                break;
            }
        }
    }
}
