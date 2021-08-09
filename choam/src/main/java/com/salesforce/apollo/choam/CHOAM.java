/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.Committee.validatorsOf;
import static com.salesforce.apollo.choam.support.HashedBlock.buildHeader;
import static com.salesforce.apollo.crypto.QualifiedBase64.bs;

import java.security.KeyPair;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Genesis;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.choam.fsm.Combine;
import com.salesforce.apollo.choam.fsm.Merchantile;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock.NullBlock;
import com.salesforce.apollo.choam.support.Store;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.utils.Channel;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.SimpleChannel;

/**
 * Combine Honnete Ober Advancer Mercantiles.
 * 
 * @author hal.hildebrand
 *
 */
public class CHOAM {
    @FunctionalInterface
    public interface BlockProducer {
        Block produce(Long height, Digest prev, Executions executions);
    }

    public class Combiner implements Combine {

        @Override
        public void awaitRegeneration() {
            // TODO Auto-generated method stub

        }

        @Override
        public void awaitSynchronization() {
            roundScheduler.schedule(AWAIT_SYNC, () -> synchronizationFailed(), 2);
        }

        @Override
        public void cancelTimer(String timer) {
            roundScheduler.cancel(timer);
        }

        @Override
        public void regenerate() {
            current.regenerate();
        }

        private void synchronizationFailed() {
            if (current.isMember()) {
                transitions.regenerate();
            } else {
                transitions.synchronizationFailed();
            }
        }

    }

    /** service trampoline */
    public class Concierge {

        public CheckpointSegments fetch(CheckpointReplication request, Digest from) {
            return CHOAM.this.fetch(request, from);
        }

        public Blocks fetchBlocks(BlockReplication request, Digest from) {
            return CHOAM.this.fetchBlocks(request, from);
        }

        public Blocks fetchViewChain(BlockReplication request, Digest from) {
            return CHOAM.this.fetchViewChain(request, from);
        }

        public ViewMember join(JoinRequest request, Digest from) {
            return CHOAM.this.join(request, from);
        }

        public SubmitResult submit(SubmitTransaction request, Digest from) {
            return CHOAM.this.submit(request, from);
        }

        public Initial sync(Synchronize request, Digest from) {
            return CHOAM.this.sync(request, from);
        }

    }

    /** a member of the current committee */
    class Associate extends Administration {
        private final Producer             producer;
        private final HashedCertifiedBlock viewChange;

        Associate(HashedCertifiedBlock block, Map<Member, Verifier> validators, nextView view) {
            super(validators);
            this.viewChange = block;
            var v = new Digest(viewChange.block.hasGenesis() ? viewChange.block.getGenesis().getInitialView().getId()
                                                             : viewChange.block.getReconfigure().getId());
            var context = Committee.viewFor(v, params.context());
            context.allMembers().filter(m -> !validators.containsKey(m)).forEach(m -> context.offline(m));
            validators.keySet().forEach(m -> context.activate(m));
            producer = new Producer(view,
                                    new ReliableBroadcaster(params.coordination().clone().setMember(params.member())
                                                                  .setContext(context).build(),
                                                            params.communications()),
                                    comm, params, reconfigureBlock(), publisher(),
                                    getReconfigure().getViewList().stream().map(vm -> new Digest(vm.getId())).toList(),
                                    constructBlock(), head);
            producer.start();
        }

        @Override
        public void complete() {
            producer.complete();
        }

        @Override
        public HashedBlock getViewChange() {
            return viewChange;
        }

        private Reconfigure getReconfigure() {
            return viewChange.block.hasGenesis() ? viewChange.block.getGenesis().getInitialView()
                                                 : viewChange.block.getReconfigure();
        }

    }

    record nextView(ViewMember member, KeyPair consensusKeyPair) {}

    /** abstract class to maintain the common state */
    private abstract class Administration implements Committee {
        private final Map<Member, Verifier> validators;

        public Administration(Map<Member, Verifier> validator) {
            this.validators = validator;
        }

        @Override
        public void accept(HashedCertifiedBlock hb) {
            process();
        }

        @Override
        public void complete() {
        }

        @Override
        public boolean isMember() {
            return validators.containsKey(params.member());
        }

        @Override
        public ViewMember join(JoinRequest request, Digest from) {
            Member source = params.context().getActiveMember(from);
            if (source == null) {
                CHOAM.log.debug("Request to join from non member: {} on: {}", from, params.member());
                return ViewMember.getDefaultInstance();
            }
            if (!validators.containsKey(source)) {
                CHOAM.log.debug("Request to join from non validator: {} on: {}", source, params.member());
                return ViewMember.getDefaultInstance();
            }
            Digest nextView = new Digest(request.getNextView());
            final Set<Member> members = Committee.viewMembersOf(nextView, params.context());
            if (!members.contains(source)) {
                CHOAM.log.debug("Request to join invalid view: {} from: {} members: {} on: {}", nextView, source,
                                members, params.member());
                return ViewMember.getDefaultInstance();
            }
            return next.member;
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
        private final Context<Member> formation;
        private final Producer        producer;

        private Formation() {
            formation = Committee.viewFor(params.genesisViewId(), params.context());
            producer = new Producer(next,
                                    new ReliableBroadcaster(params.coordination().clone().setMember(params.member())
                                                                  .setContext(formation).build(),
                                                            params.communications()),
                                    comm, params, genesisBlock(), publisher(), Collections.emptyList(),
                                    constructBlock(), null);
            producer.setNextViewId(params.genesisViewId());
        }

        @Override
        public void accept(HashedCertifiedBlock hb) {
            genesis = head;
            checkpoint = head;
            view = head;
            process();
        }

        @Override
        public void complete() {
            producer.complete();
        }

        @Override
        public HashedBlock getViewChange() {
            assert genesis != null;
            return genesis;
        }

        @Override
        public boolean isMember() {
            return formation.isActive(params.member());
        }

        @Override
        public ViewMember join(JoinRequest request, Digest from) {
            Member source = formation.getActiveMember(from);
            if (source == null) {
                CHOAM.log.debug("Request to join from non validator: {} on: {}", from, params.member());
                return ViewMember.getDefaultInstance();
            }
            Digest nextView = new Digest(request.getNextView());
            if (!params.genesisViewId().equals(nextView)) {
                CHOAM.log.debug("Request to join invalid view: {} from: {} on: {}", nextView, source, params.member());
                return ViewMember.getDefaultInstance();
            }
            return next.member;
        }

        @Override
        public Parameters params() {
            return params;
        }

        @Override
        public void regenerate() {
            producer.regenerate();
        }

        @Override
        public boolean validate(HashedCertifiedBlock hb) {
            var block = hb.block;
            if (!block.hasGenesis()) {
                CHOAM.log.debug("Invalid genesis block: {} on: {}", hb.hash, params.member());
                return false;
            }
            return validateRegeneration(hb);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(CHOAM.class);

    public static Block genesis(Digest id, Map<Member, Join> joins, HashedBlock head, Context<Member> context,
                                HashedBlock lastViewChange, Parameters params, HashedBlock lastCheckpoint,
                                Iterable<? extends ExecutedTransaction> initialization) {
        var reconfigure = reconfigure(id, joins, context, params);
        return Block.newBuilder()
                    .setHeader(buildHeader(params.digestAlgorithm(), reconfigure, head.hash, head.height() + 1,
                                           lastCheckpoint.height(), lastCheckpoint.hash, lastViewChange.height(),
                                           lastViewChange.hash))
                    .setGenesis(Genesis.newBuilder().setInitialView(reconfigure).addAllInitialize(initialization))
                    .build();
    }

    public static Reconfigure reconfigure(Digest id, Map<Member, Join> joins, Context<Member> context,
                                          Parameters params) {
        var builder = Reconfigure.newBuilder().setCheckpointBlocks(params.checkpointBlockSize()).setId(id.toDigeste())
                                 .setEpochLength(params.ethereal().getEpochLength())
                                 .setNumberOfEpochs(params.ethereal().getNumberOfEpochs());

        // Canonical labeling of the view members for Ethereal
        var ring0 = context.ring(0);
        var remapped = joins.keySet().stream().collect(Collectors.toMap(m -> ring0.hash(m), m -> m));
        remapped.keySet().stream().sorted().map(d -> remapped.get(d)).peek(m -> builder.addJoins(joins.get(m)))
                .forEach(m -> builder.addView(joins.get(m).getMember()));

        var reconfigure = builder.build();
        return reconfigure;
    }

    public static Block reconfigure(Digest id, Map<Member, Join> joins, HashedBlock head, Context<Member> context,
                                    HashedBlock lastViewChange, Parameters params, HashedBlock lastCheckpoint) {
        var reconfigure = reconfigure(id, joins, context, params);
        return Block.newBuilder()
                    .setHeader(buildHeader(params.digestAlgorithm(), reconfigure, head.hash, head.height() + 1,
                                           lastCheckpoint.height(), lastCheckpoint.hash, lastViewChange.height(),
                                           lastViewChange.hash))
                    .setReconfigure(reconfigure).build();
    }

    private volatile HashedCertifiedBlock                   checkpoint;
    private final ReliableBroadcaster                       combine;
    private final CommonCommunications<Terminal, Concierge> comm;
    private volatile Committee                              current;
    private final Fsm<Combine, Combine.Transitions>         fsm;
    private volatile HashedCertifiedBlock                   genesis;
    private volatile HashedCertifiedBlock                   head;
    private final Channel<List<Msg>>                        linear;
    private volatile nextView                               next;
    private final Parameters                                params;
    private final PriorityQueue<HashedCertifiedBlock>       pending = new PriorityQueue<>();
    private final RoundScheduler                            roundScheduler;
    private final AtomicBoolean                             started = new AtomicBoolean();
    private final Store                                     store;
    private final Combine.Transitions                       transitions;
    private HashedCertifiedBlock                            view;

    public CHOAM(Parameters params, MVStore store) {
        this(params, new Store(params.digestAlgorithm(), store));
    }

    public CHOAM(Parameters params, Store store) {
        this.store = store;
        this.params = params;
        nextView();
        combine = new ReliableBroadcaster(params.combineParameters().setMember(params.member())
                                                .setContext(params.context()).build(),
                                          params.communications());
        linear = new SimpleChannel<>("Linear for: " + params.member(), 100);
        combine.registerHandler((ctx, messages) -> linear.submit(messages));
        head = new NullBlock(params.digestAlgorithm());
        view = new NullBlock(params.digestAlgorithm());
        checkpoint = new NullBlock(params.digestAlgorithm());
        comm = params.communications()
                     .create(params.member(), params.context().getId(), new Concierge(),
                             r -> new TerminalServer(params.communications().getClientIdentityProvider(),
                                                     params.metrics(), r),
                             TerminalClient.getCreate(params.metrics()), Terminal.getLocalLoopback(params.member()));
        fsm = Fsm.construct(new Combiner(), Combine.Transitions.class, Merchantile.INITIAL, true);
        fsm.setName("CHOAM" + params.member().getId() + params.context().getId());
        transitions = fsm.getTransitions();
        roundScheduler = new RoundScheduler(params.context().getRingCount());
        combine.register(i -> roundScheduler.tick(i));
        current = new Formation();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        linear.open();
        linear.consumeEach(msgs -> combine(msgs));
        combine.start(params.gossipDuration(), params.scheduler());
        fsm.enterStartState();
        transitions.start();
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
        log.info("Accepted block: {} height: {} on: {}", next.hash, next.height(), params.member());
        final Committee c = current;
        c.accept(next);
    }

    private void checkpoint() {
        // TODO Auto-generated method stub

    }

    private void combine() {
        log.trace("Attempting to combine blocks on: {}", params.member());
        var next = pending.peek();
        while (next != null) {
            final HashedCertifiedBlock h = head;
            if (h.height() >= 0 && next.height() <= h.height()) {
                log.trace("Have already advanced beyond block: {} height: {} current: {} on: {}", next.hash,
                          next.height(), h.height(), params.member());
                pending.poll();
            } else if (isNext(next)) {
                if (current.validate(next)) {
                    HashedCertifiedBlock nextBlock = pending.poll();
                    if (nextBlock == null) {
                        return;
                    }
                    accept(nextBlock);
                } else {
                    log.info("Unable to validate block: {} height: {} on: {}", next.hash, next.height(),
                             params.member());
                    pending.poll();
                }
            } else {
                log.trace("Premature block: {} height: {} current: {} on: {}", next.hash, next.height(), h.height(),
                          params.member());
                return;
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
        HashedCertifiedBlock hcb = new HashedCertifiedBlock(params.digestAlgorithm(), block);
        log.trace("Received block: {} height: {} from {} on: {}", hcb.hash, hcb.height(), m.source(), params.member());
        pending.add(hcb);
    }

    private BlockProducer constructBlock() {
        return (height, prev, executions) -> Block.newBuilder()
                                                  .setHeader(buildHeader(params.digestAlgorithm(), executions, prev,
                                                                         height, checkpoint.height(), checkpoint.hash,
                                                                         view.height(), view.hash))
                                                  .setExecutions(executions).build();
    }

    private CheckpointSegments fetch(CheckpointReplication request, Digest from) {
        // TODO Auto-generated method stub
        return null;
    }

    private Blocks fetchBlocks(BlockReplication request, Digest from) {
        // TODO Auto-generated method stub
        return null;
    }

    private Blocks fetchViewChain(BlockReplication request, Digest from) {
        // TODO Auto-generated method stub
        return null;
    }

    private BiFunction<Map<Member, Join>, Digest, Block> genesisBlock() {
        return (joining, nextViewId) -> CHOAM.genesis(nextViewId, joining, head, params.context(), view, params,
                                                      checkpoint, params.genesisData());
    }

    private boolean isNext(HashedBlock next) {
        boolean isNext = next != null && next.height() == head.height() + 1 && head.hash.equals(next.getPrevious());
//        log.info("Block: {} height: {} prev: {} isNext: {} current: {} height: {} on: {}", next.hash, next.height(),
//                  new Digest(next.block.getHeader().getPrevious()), isNext, head.hash, head.height(), params.member());
        return isNext;
    }

    private ViewMember join(JoinRequest request, Digest from) {
        return current.join(request, from);
    }

    private void nextView() {
        log.trace("Generating next view consensus key on: {}", params.member());
        KeyPair keyPair = params.viewSigAlgorithm().generateKeyPair();
        PubKey pubKey = bs(keyPair.getPublic());
        JohnHancock signed = params.member().sign(pubKey.toByteString());
        if (signed == null) {
            log.error("Unable to generate and sign consensus key on: {}", params.member());
            return;
        }
        next = new nextView(ViewMember.newBuilder().setId(params.member().getId().toDigeste()).setConsensusKey(pubKey)
                                      .setSignature(signed.toSig()).build(),
                            keyPair);
    }

    private void process() {
        final HashedBlock h = head;
        switch (h.block.getBodyCase()) {
        case CHECKPOINT:
            checkpoint();
            break;
        case RECONFIGURE:
            reconfigure(h.block.getReconfigure());
            break;
        case GENESIS:
            reconfigure(h.block.getGenesis().getInitialView());
        case EXECUTIONS:
            params.processor().accept(head);
            break;
        default:
            break;
        }
    }

    private Consumer<HashedCertifiedBlock> publisher() {
        return cb -> combine.publish(cb.certifiedBlock.toByteArray(), true);
    }

    private void reconfigure(Reconfigure reconfigure) {
        final Committee c = current;
        c.complete();
        var validators = validatorsOf(reconfigure, params.context());
        var currentView = next;
        nextView();
        final HashedCertifiedBlock h = head;
        if (validators.containsKey(params.member())) {
            current = new Associate(h, validators, currentView);
        } else {
            current = new Client(h, validators);
        }
        log.info("Reconfigured to view: {} on: {}", new Digest(reconfigure.getId()), params.member());
    }

    private BiFunction<Map<Member, Join>, Digest, Block> reconfigureBlock() {
        return (joining, nextViewId) -> {
            final HashedCertifiedBlock h = head;
            final HashedCertifiedBlock v = view;
            final HashedCertifiedBlock c = checkpoint;
            return CHOAM.reconfigure(nextViewId, joining, h, params.context(), v, params, c);
        };
    }

    /** Submit a transaction from a client */
    private SubmitResult submit(SubmitTransaction request, Digest from) {
        // TODO Auto-generated method stub
        return null;
    }

    private Initial sync(Synchronize request, Digest from) {
        // TODO Auto-generated method stub
        return null;
    }
}
