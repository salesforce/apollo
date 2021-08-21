/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.Committee.validatorsOf;
import static com.salesforce.apollo.choam.support.HashedBlock.buildHeader;
import static com.salesforce.apollo.choam.support.HashedBlock.height;
import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static com.salesforce.apollo.crypto.QualifiedBase64.digest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Checkpoint;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Genesis;
import com.salesfoce.apollo.choam.proto.Header;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Outcome;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.choam.fsm.Combine;
import com.salesforce.apollo.choam.fsm.Merchantile;
import com.salesforce.apollo.choam.support.Bootstrapper;
import com.salesforce.apollo.choam.support.Bootstrapper.SynchronizedState;
import com.salesforce.apollo.choam.support.CheckpointState;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock.NullBlock;
import com.salesforce.apollo.choam.support.ServiceUnavailable;
import com.salesforce.apollo.choam.support.Store;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.GroupIterator;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.utils.Channel;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.SimpleChannel;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

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
            final HashedCertifiedBlock g = genesis;
            if (g != null) {
                return;
            }
            HashedCertifiedBlock anchor = pending.poll();
            if (anchor != null) {
                log.info("Recovering from anchor: {} on: {}", anchor.hash, params.member());
                recover(anchor);
                return;
            }
            log.debug("No anchor to recover from on: {}", params.member());
            roundScheduler.schedule(AWAIT_SYNC, () -> {
                futureSynchronization = null;
                awaitRegeneration();
            }, params.regenerationCycles());
        }

        @Override
        public void awaitSynchronization() {
            HashedCertifiedBlock anchor = pending.poll();
            if (anchor != null) {
                log.info("Recovering from anchor: {} on: {}", anchor.hash, params.member());
                recover(anchor);
                return;
            }
            log.debug("No anchor to recover from on: {}", params.member());
            roundScheduler.schedule(AWAIT_SYNC, () -> {
                futureSynchronization = null;
                synchronizationFailed();
            }, params.synchronizationCycles());
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

    public class Trampoline implements Concierge {

        @Override
        public CheckpointSegments fetch(CheckpointReplication request, Digest from) {
            return CHOAM.this.fetch(request, from);
        }

        @Override
        public Blocks fetchBlocks(BlockReplication request, Digest from) {
            return CHOAM.this.fetchBlocks(request, from);
        }

        @Override
        public Blocks fetchViewChain(BlockReplication request, Digest from) {
            return CHOAM.this.fetchViewChain(request, from);
        }

        @Override
        public ViewMember join(JoinRequest request, Digest from) {
            return CHOAM.this.join(request, from);
        }

        @Override
        public SubmitResult submit(SubmitTransaction request, Digest from) {
            return CHOAM.this.submit(request, from);
        }

        @Override
        public Initial sync(Synchronize request, Digest from) {
            return CHOAM.this.sync(request, from);
        }

    }

    public interface TransactionExecutor {
        default void beginBlock(long height, Digest hash) {
        }

        @SuppressWarnings("rawtypes")
        void execute(ExecutedTransaction tx, CompletableFuture onComplete);
    }

    /** a member of the current committee */
    class Associate extends Administration {
        private final Producer producer;

        Associate(HashedCertifiedBlock viewChange, Map<Member, Verifier> validators, nextView nextView) {
            super(validators,
                  new Digest(viewChange.block.hasGenesis() ? viewChange.block.getGenesis().getInitialView().getId()
                                                           : viewChange.block.getReconfigure().getId()));
            var context = Committee.viewFor(viewId, params.context());
            context.allMembers().filter(m -> !validators.containsKey(m)).forEach(m -> context.offline(m));
            validators.keySet().forEach(m -> context.activate(m));
            Signer signer = new SignerImpl(0, nextView.consensusKeyPair.getPrivate());
            ViewContext viewContext = new ViewContext(context, params, signer, validators, publisher(),
                                                      constructBlock());
            producer = new Producer(viewContext,
                                    new ReliableBroadcaster(params.coordination().clone().setMember(params.member())
                                                                  .setContext(context).build(),
                                                            params.communications()),
                                    comm, head, reconfigureBlock());
            producer.start();
        }

        @Override
        public void complete() {
            producer.complete();
        }

        @Override
        public SubmitResult submit(SubmitTransaction request) {
            CHOAM.log.trace("Submit txn on: {}", params().member());
            return producer.submit(request.getTransaction());
        }
    }

    record nextView(ViewMember member, KeyPair consensusKeyPair) {}

    @FunctionalInterface
    interface ReconfigureBlock {
        Block reconfigure(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous);
    }

    /** abstract class to maintain the common state */
    private abstract class Administration implements Committee {
        private GroupIterator servers;
        protected Digest      viewId;

        private final Map<Member, Verifier> validators;

        public Administration(Map<Member, Verifier> validators, Digest viewId) {
            this.validators = validators;
            this.viewId = viewId;
            servers = new GroupIterator(validators.keySet());
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
            if (!members.contains(params.member())) {
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
        public void submitTxn(Transaction transaction, CompletableFuture<Boolean> result) {
            Member target = servers.next();
            ListenableFuture<SubmitResult> response;
            try (var link = comm.apply(target, params.member())) {
                if (link == null) {
                    CHOAM.log.trace("No link for: {} for submitting txn on: {}", target.getId(), params.member());
                    result.completeExceptionally(new ServiceUnavailable());
                    return;
                }
                CHOAM.log.trace("Submitting txn to: {} in: {} on: {}", target.getId(), viewId, params.member());
                response = link.submit(SubmitTransaction.newBuilder().setContext(params.context().getId().toDigeste())
                                                        .setTransaction(transaction).build());
            } catch (Throwable e) {
                CHOAM.log.trace("Failed submitting txn to: {} in: {} on: {}", target.getId(), viewId, params.member(),
                                e);
                result.completeExceptionally(e);
                return;
            }
            response.addListener(() -> {
                SubmitResult resulting;
                try {
                    resulting = response.get();
                } catch (InterruptedException e) {
                    CHOAM.log.debug("Failed submitting txn to: {} in: {} on: {}", target.getId(), viewId,
                                    params.member(), e);
                    result.completeExceptionally(e.getCause());
                    return;
                } catch (ExecutionException e) {
                    CHOAM.log.trace("Failed submitting txn to: {} in: {} on: {}", target.getId(), viewId,
                                    params.member(), e.getCause());
                    result.completeExceptionally(e.getCause());
                    return;
                }
                if (!resulting.isInitialized()) {
                    CHOAM.log.trace("Null response submitting txn to: {} in: {} on: {}", target.getId(), viewId,
                                    params.member());
                    result.completeExceptionally(new ServiceUnavailable());
                    return;
                }
                if (resulting.getOutcome() != Outcome.SUCCESS) {
                    CHOAM.log.trace("Failed submitting txn to: {} in: {} on: {}", target.getId(), viewId,
                                    params.member());
                    result.completeExceptionally(new ServiceUnavailable());
                } else {
                    CHOAM.log.debug("Success submitting txn to: {} in: {} on: {}", target.getId(), viewId,
                                    params.member());
                    result.complete(true);
                }
            }, params.dispatcher());
        }

        @Override
        public boolean validate(HashedCertifiedBlock hb) {
            return validate(hb, validators);
        }
    }

    /** a client of the current committee */
    private class Client extends Administration {

        public Client(Map<Member, Verifier> validators, Digest viewId) {
            super(validators, viewId);
        }
    }

    /** The Genesis formation comittee */
    private class Formation implements Committee {
        private final Context<Member>     formation;
        private final ViewReconfiguration reconfigure;

        private Formation() {
            formation = Committee.viewFor(params.genesisViewId(), params.context());
            if (formation.isActive(params.member())) {
                Signer signer = new SignerImpl(0, next.consensusKeyPair.getPrivate());
                ViewContext vc = new GenesisContext(formation, params, signer, publisher());
                reconfigure = new ViewReconfiguration(params.genesisViewId(), vc, head, comm, genesisBlock());
            } else {
                reconfigure = null;
            }
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
            if (reconfigure != null) {
                reconfigure.complete();
            }
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
            if (reconfigure != null) {
                reconfigure.start();
            }
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

    /** a synchronizer of the current committee */
    private class Synchronizer extends Administration {
        public Synchronizer(Map<Member, Verifier> validators) {
            super(validators, null);

        }
    }

    /** a no op committee during synchronization */
    @SuppressWarnings("unused")
    private class Synchronizing extends Administration {
        public Synchronizing() {
            super(Collections.emptyMap(), null);
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
        var remapped = rosterMap(context, joins.keySet());

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

    public static Map<Digest, Member> rosterMap(Context<Member> baseContext, Collection<Member> members) {

        // Canonical labeling of the view members for Ethereal
        var ring0 = baseContext.ring(0);
        return members.stream().collect(Collectors.toMap(m -> ring0.hash(m), m -> m));
    }

    private final Map<Long, CheckpointState>                cachedCheckpoints = new ConcurrentHashMap<>();
    private volatile HashedCertifiedBlock                   checkpoint;
    private final ReliableBroadcaster                       combine;
    private final CommonCommunications<Terminal, Concierge> comm;
    private volatile Committee                              current;
    private volatile CompletableFuture<SynchronizedState>   futureBootstrap;
    private volatile ScheduledFuture<?>                     futureSynchronization;
    private volatile HashedCertifiedBlock                   genesis;
    private volatile HashedCertifiedBlock                   head;
    private final Channel<List<Msg>>                        linear;
    private volatile nextView                               next;
    private final Parameters                                params;
    private final PriorityQueue<HashedCertifiedBlock>       pending           = new PriorityQueue<>();
    private final RoundScheduler                            roundScheduler;
    private final Session                                   session;
    private final AtomicBoolean                             started           = new AtomicBoolean();
    private final Store                                     store;
    private final AtomicBoolean                             synchronizing     = new AtomicBoolean(false);
    private final Combine.Transitions                       transitions;

    private volatile HashedCertifiedBlock view;

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
                     .create(params.member(), params.context().getId(), new Trampoline(),
                             r -> new TerminalServer(params.communications().getClientIdentityProvider(),
                                                     params.metrics(), r),
                             TerminalClient.getCreate(params.metrics()), Terminal.getLocalLoopback(params.member()));
        var fsm = Fsm.construct(new Combiner(), Combine.Transitions.class, Merchantile.INITIAL, true);
        fsm.setName("CHOAM" + params.member().getId() + params.context().getId());
        transitions = fsm.getTransitions();
        roundScheduler = new RoundScheduler(params.context().getRingCount());
        combine.register(i -> roundScheduler.tick(i));
        current = new Formation();
        session = params.session().build(params, service());
    }

    public Session getSession() {
        return session;
    }

    public Digest getViewId() {
        final var viewChange = view;
        if (viewChange == null) {
            return null;
        }
        return new Digest(viewChange.block.hasGenesis() ? viewChange.block.getGenesis().getInitialView().getId()
                                                        : viewChange.block.getReconfigure().getId());
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        linear.open();
        linear.consumeEach(msgs -> combine(msgs));
        combine.start(params.gossipDuration(), params.scheduler());
        transitions.fsm().enterStartState();
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
        log.debug("Accepted block: {} height: {} on: {}", next.hash, next.height(), params.member());
        final Committee c = current;
        c.accept(next);
    }

    private Bootstrapper bootstrapper(HashedCertifiedBlock anchor) {
        return new Bootstrapper(anchor, params, store, comm);
    }

    private void cancelSynchronization() {
        final ScheduledFuture<?> fs = futureSynchronization;
        if (fs != null) {
            fs.cancel(true);
            futureSynchronization = null;
        }
        final CompletableFuture<SynchronizedState> fb = futureBootstrap;
        if (fb != null) {
            fb.cancel(true);
            futureBootstrap = null;
        }
    }

    private void checkpoint() {
        CheckpointState checkpointState = checkpoint(head.block.getCheckpoint(), head.height());
        if (checkpointState == null) {
            log.error("Cannot checkpoint: {} on: {}", head.hash, params.member());
            transitions.fail();
            return;
        }
        cachedCheckpoints.put(head.height(), checkpointState);
    }

    private CheckpointState checkpoint(Checkpoint body, long height) {
        Digest stateHash;
        CheckpointState checkpoint = cachedCheckpoints.get(height);
        Digest bsh = new Digest(body.getStateHash());
        if (checkpoint != null) {
            if (!body.getStateHash().equals(checkpoint.checkpoint.getStateHash())) {
                log.error("Invalid checkpoint state hash: {} does not equal recorded: {} on: {}",
                          new Digest(checkpoint.checkpoint.getStateHash()), bsh, params.member());
                return null;
            }
        } else {
            File state = params.checkpointer().apply(height - 1);
            if (state == null) {
                log.error("Invalid checkpoint on: {}", params.member());
                return null;
            }
            try (FileInputStream fis = new FileInputStream(state)) {
                stateHash = params.digestAlgorithm().digest(fis);
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
            if (!stateHash.equals(bsh)) {
                log.error("Cannot replicate checkpoint: {} state hash: {} does not equal recorded: {} on: {}", height,
                          stateHash, bsh, params.member());
                state.delete();
                return null;
            }
            MVMap<Integer, byte[]> stored = store.putCheckpoint(height, state, body);
            checkpoint = new CheckpointState(body, stored);
            state.delete();
        }
        return checkpoint;
    }

    private void combine() {
        log.trace("Attempting to combine blocks on: {}", params.member());
        var next = pending.peek();
        while (next != null) {
            final HashedCertifiedBlock h = head;
            if (h.height() >= 0 && next.height() <= h.height()) {
//                log.trace("Have already advanced beyond block: {} height: {} current: {} on: {}", next.hash,
//                          next.height(), h.height(), params.member());
                pending.poll();
            } else if (isNext(next)) {
                if (current.validate(next)) {
                    HashedCertifiedBlock nextBlock = pending.poll();
                    if (nextBlock == null) {
                        return;
                    }
                    accept(nextBlock);
                } else {
                    log.debug("Unable to validate block: {} height: {} on: {}", next.hash, next.height(),
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

    private void execute(List<ExecutedTransaction> executions) {
        log.trace("Executing transactions for block: {} height: {}  on: {}", head.hash, head.height(), params.member());
        params.processor().beginBlock(head.height(), head.hash);
        executions.forEach(exec -> {
            Digest hash = params.digestAlgorithm().digest(exec.getTransation().toByteString());
            var stxn = session.complete(hash);
            log.trace("Executing transaction: {} block: {} height: {} stxn: {} on: {}", hash, head.hash, head.height(),
                      stxn == null ? "null" : "present", params.member());
            try {
                params.processor().execute(exec, stxn == null ? null : stxn.onCompletion());
            } catch (Throwable t) {
                log.error("Exception processing transaction: {} block: {} height: {} on: {}", hash, head.hash,
                          head.height(), params.member());
            }
        });
    }

    private CheckpointSegments fetch(CheckpointReplication request, Digest from) {
        Member member = params.context().getMember(from);
        if (member == null) {
            log.warn("Received checkpoint fetch from non member: {} on: {}", from, params.member());
            return CheckpointSegments.getDefaultInstance();
        }
        CheckpointState state = cachedCheckpoints.get(request.getCheckpoint());
        if (state == null) {
            log.info("No cached checkpoint for {} on: {}", request.getCheckpoint(), params.member());
            return CheckpointSegments.getDefaultInstance();
        }
        CheckpointSegments.Builder replication = CheckpointSegments.newBuilder();

        return replication.addAllSegments(state.fetchSegments(BloomFilter.from(request.getCheckpointSegments()),
                                                              params.maxCheckpointSegments(), Utils.bitStreamEntropy()))
                          .build();
    }

    private Blocks fetchBlocks(BlockReplication rep, Digest from) {
        Member member = params.context().getMember(from);
        if (member == null) {
            log.warn("Received fetchBlocks from non member: {} on: {}", from, params.member());
            return Blocks.getDefaultInstance();
        }
        BloomFilter<Long> bff = BloomFilter.from(rep.getBlocksBff());
        Blocks.Builder blocks = Blocks.newBuilder();
        store.fetchBlocks(bff, blocks, 5, rep.getFrom(), rep.getTo());
        return blocks.build();
    }

    private Blocks fetchViewChain(BlockReplication rep, Digest from) {
        Member member = params.context().getMember(from);
        if (member == null) {
            log.warn("Received fetchViewChain from non member: {} on: {}", from, params.member());
            return Blocks.getDefaultInstance();
        }
        BloomFilter<Long> bff = BloomFilter.from(rep.getBlocksBff());
        Blocks.Builder blocks = Blocks.newBuilder();
        store.fetchViewChain(bff, blocks, 1, rep.getFrom(), rep.getTo());
        return blocks.build();
    }

    private ReconfigureBlock genesisBlock() {
        return (joining, nextViewId, last) -> CHOAM.genesis(nextViewId, joining, last, params.context(), view, params,
                                                            checkpoint, params.genesisData());
    }

    private boolean isNext(HashedBlock next) {
        return next != null && next.height() == head.height() + 1 && head.hash.equals(next.getPrevious());
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
            cancelSynchronization();
            reconfigure(h.block.getGenesis().getInitialView());
            execute(head.block.getGenesis().getInitializeList());
        case EXECUTIONS:
            execute(head.block.getExecutions().getExecutionsList());
            break;
        default:
            break;
        }
    }

    private Consumer<HashedCertifiedBlock> publisher() {
        return cb -> combine.publish(cb.certifiedBlock, true);
    }

    private void reconfigure(Reconfigure reconfigure) {
        final Committee c = current;
        c.complete();
        var validators = validatorsOf(reconfigure, params.context());
        var currentView = next;
        nextView();
        final HashedCertifiedBlock h = head;
        view = h;
        if (validators.containsKey(params.member())) {
            current = new Associate(h, validators, currentView);
        } else {
            current = new Client(validators, getViewId());
        }
        log.info("Reconfigured to view: {} on: {}", new Digest(reconfigure.getId()), params.member());
    }

    private ReconfigureBlock reconfigureBlock() {
        return (joining, nextViewId, previous) -> {
            final HashedCertifiedBlock v = view;
            final HashedCertifiedBlock c = checkpoint;
            return reconfigure(nextViewId, joining, previous, params.context(), v, params, c);
        };
    }

    private void recover(HashedCertifiedBlock anchor) {
        if (futureSynchronization != null) {
            futureSynchronization.cancel(true);
            futureSynchronization = null;
        }
        futureBootstrap = bootstrapper(anchor).synchronize().whenComplete((s, t) -> {
            if (t == null) {
                try {
                    synchronize(s);
                } catch (Throwable e) {
                    log.error("Cannot synchronize on: {}", params.member(), e);
                    transitions.fail();
                }
            } else {
                log.error("Synchronization failed on: {}", params.member(), t);
                transitions.fail();
            }
        }).exceptionally(t -> {
            log.error("Synchronization failed on: {}", params.member(), t);
            transitions.fail();
            return null;
        }).orTimeout(params.synchronizeTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void restore() throws IllegalStateException {
        HashedCertifiedBlock lastBlock = store.getLastBlock();
        if (lastBlock == null) {
            log.info("No state to restore from on: {}", params.member().getId());
            return;
        }
        genesis = new HashedCertifiedBlock(params.digestAlgorithm(), store.getCertifiedBlock(0));
        head = lastBlock;
        Header header = lastBlock.block.getHeader();
        HashedCertifiedBlock lastView = new HashedCertifiedBlock(params.digestAlgorithm(),
                                                                 store.getCertifiedBlock(header.getLastReconfig()));
        Reconfigure reconfigure = lastView.block.getReconfigure();
        view = lastView;
        var validators = validatorsOf(reconfigure, params.context());
        current = new Synchronizer(validators);
        log.info("Reconfigured to view: {} on: {}", new Digest(reconfigure.getId()), params.member());
        CertifiedBlock lastCheckpoint = store.getCertifiedBlock(header.getLastCheckpoint());
        if (lastCheckpoint != null) {
            checkpoint = new HashedCertifiedBlock(params.digestAlgorithm(), lastCheckpoint);
        }

        log.info("Restored to: {} lastView: {} lastCheckpoint: {} lastBlock: {} on: {}",
                 new HashedCertifiedBlock(params.digestAlgorithm(), store.getCertifiedBlock(0)).hash, lastView.hash,
                 lastCheckpoint == null ? "<missing>"
                                        : new HashedCertifiedBlock(params.digestAlgorithm(), lastCheckpoint).hash,
                 lastBlock.hash, params.member().getId());
    }

    private void restoreFrom(HashedCertifiedBlock block, CheckpointState checkpoint) {
        cachedCheckpoints.put(block.height(), checkpoint);
        params.restorer().accept(block.height(), checkpoint);
        restore();
        checkpoint();
    }

    private Function<SubmittedTransaction, Boolean> service() {
        return stx -> {
            log.trace("Submitting transaction: {} on: {}", stx.hash(), params.member());
            final var c = current;
            if (c == null) {
                throw new ServiceUnavailable();
            }
            CompletableFuture<Boolean> result = new CompletableFuture<>();
            c.submitTxn(stx.transaction(), result);
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new ServiceUnavailable(e);
            }
        };
    }

    /** Submit a transaction from a client */
    private SubmitResult submit(SubmitTransaction request, Digest from) {
        if (params.context().getMember(from) == null) {
            log.debug("Invalid transaction submission from non member: {} on: {}", from, params.member());
            return SubmitResult.newBuilder().setOutcome(Outcome.NOT_A_MEMBER).build();
        }
        final var c = current;
        if (c == null) {
            log.debug("No committee to submit txn from: {} on: {}", from, params.member());
            return SubmitResult.newBuilder().setOutcome(Outcome.INACTIVE_COMMITTEE).build();
        }
        log.trace("Submiting txn from: {} on: {}", from, params.member());
        return c.submit(request);
    }

    private Initial sync(Synchronize request, Digest from) {
        Member member = params.context().getMember(from);
        if (member == null) {
            log.warn("Received sync from non member: {} on: {}", from, params.member());
            return Initial.getDefaultInstance();
        }
        Initial.Builder initial = Initial.newBuilder();
        final HashedCertifiedBlock g = genesis;
        if (g != null) {
            initial.setGenesis(g.certifiedBlock);
            HashedCertifiedBlock cp = checkpoint;
            if (cp != null) {
                long height = request.getHeight();

                while (cp.height() > height) {
                    cp = new HashedCertifiedBlock(params.digestAlgorithm(),
                                                  store.getCertifiedBlock(cp.block.getHeader().getLastCheckpoint()));
                }
                final long lastReconfig = cp.block.getHeader().getLastReconfig();
                HashedCertifiedBlock lastView = null;
                if (lastReconfig < 0) {
                    lastView = cp;
                } else {
                    var stored = store.getCertifiedBlock(lastReconfig);
                    if (stored != null) {
                        lastView = new HashedCertifiedBlock(params.digestAlgorithm(), stored);
                    }
                }
                if (lastView == null) {
                    lastView = g;
                }
                initial.setCheckpoint(cp.certifiedBlock).setCheckpointView(lastView.certifiedBlock);

                log.debug("Returning sync: {} view: {} chkpt: {} to: {} on: {}", g.hash, lastView.hash, cp.hash, from,
                          params.member());
            } else {
                log.debug("Returning sync: {} to: {} on: {}", g.hash, from, params.member());
            }
        } else {
            log.debug("Returning null sync to: {} on: {}", from, params.member());
        }
        return initial.build();
    }

    private void synchronize(SynchronizedState state) {
        synchronizing.set(true);
        transitions.synchronizing();
        CertifiedBlock current1;
        if (state.lastCheckpoint == null) {
            log.info("Synchronizing from genesis: {} on: {}", state.genesis.hash, params.member());
            current1 = state.genesis.certifiedBlock;
        } else {
            log.info("Synchronizing from checkpoint: {} on: {}", state.lastCheckpoint.hash, params.member());
            restoreFrom(state.lastCheckpoint, state.checkpoint);
            current1 = store.getCertifiedBlock(state.lastCheckpoint.height() + 1);
        }
        while (current1 != null) {
            synchronizedProcess(current1, false);
            current1 = store.getCertifiedBlock(height(current1.getBlock()) + 1);
        }
        synchronizing.set(false);
        log.info("Synchronized, resuming view on: {}",
                 state.lastCheckpoint != null ? state.lastCheckpoint.hash : state.genesis.hash, params.member());
        log.info("Processing deferred blocks: {} on: {}", pending.size(), params.member());
        combine();
    }

    private void synchronizedProcess(CertifiedBlock certifiedBlock, boolean combine) {
        if (!started.get()) {
            log.info("Not started on: {}", params.member());
            return;
        }
        HashedCertifiedBlock hcb = new HashedCertifiedBlock(params.digestAlgorithm(), certifiedBlock);
        Block block = hcb.block;
        log.debug("Processing block {} : {} height: {} on: {}", hcb.hash, block.getBodyCase(), hcb.height(),
                  params.member());
        final HashedCertifiedBlock previousBlock = head;
        Header header = block.getHeader();
        if (previousBlock != null) {
            Digest prev = digest(header.getPrevious());
            long prevHeight = previousBlock.height();
            if (hcb.height() <= prevHeight) {
                log.debug("Discarding previously committed block: {} height: {} current height: {} on: {}", hcb.hash,
                          hcb.height(), prevHeight, params.member());
                return;
            }
            if (hcb.height() != prevHeight + 1) {
                pending.add(hcb);
                log.debug("Deferring block on {}.  Block: {} height should be {} and block height is {}",
                          params.member(), hcb.hash, previousBlock.height() + 1, header.getHeight());
                return;
            }
            if (!previousBlock.hash.equals(prev)) {
                log.error("Protocol violation on {}. New block does not refer to current block hash. Should be {} and next block's prev is {}, current height: {} next height: {}",
                          params.member(), previousBlock.hash, prev, prevHeight, hcb.height());
                return;
            }
            if (!current.validate(hcb)) {
                log.error("Protocol violation on {}. New block is not validated {}", params.member(), hcb.hash);
                return;
            }
        } else {
            if (!block.hasGenesis()) {
                pending.add(hcb);
                log.info("Deferring block on {}.  Block: {} height should be {} and block height is {}",
                         params.member(), hcb.hash, 0, header.getHeight());
                return;
            }
            if (!current.validateRegeneration(hcb)) {
                log.error("Protocol violation on: {}. Genesis block is not validated {}", params.member(), hcb.hash);
                return;
            }
        }
        pending.add(hcb);
        if (combine) {
            combine();
        }
    }
}
