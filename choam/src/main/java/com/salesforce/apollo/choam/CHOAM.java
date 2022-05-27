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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.h2.mvstore.MVMap;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.base.Function;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Assemble;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Checkpoint;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Genesis;
import com.salesfoce.apollo.choam.proto.Header;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Result;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Submitter;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.choam.comm.TxnSubmission;
import com.salesforce.apollo.choam.comm.TxnSubmitClient;
import com.salesforce.apollo.choam.comm.TxnSubmitServer;
import com.salesforce.apollo.choam.fsm.Combine;
import com.salesforce.apollo.choam.fsm.Merchantile;
import com.salesforce.apollo.choam.support.Bootstrapper;
import com.salesforce.apollo.choam.support.Bootstrapper.SynchronizedState;
import com.salesforce.apollo.choam.support.CheckpointState;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock.NullBlock;
import com.salesforce.apollo.choam.support.Store;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.GroupIterator;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

import io.grpc.StatusRuntimeException;

/**
 * Combine Honnete Ober Advancer Mercantiles.
 * 
 * @author hal.hildebrand
 *
 */
public class CHOAM {
    public interface BlockProducer {
        Block checkpoint();

        Block genesis(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous);

        Block produce(ULong height, Digest prev, Assemble assemble, HashedBlock checkpoint);

        Block produce(ULong height, Digest prev, Executions executions, HashedBlock checkpoint);

        void publish(CertifiedBlock cb);

        Block reconfigure(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous, HashedBlock checkpoint);
    }

    public class Combiner implements Combine {

        @Override
        public void anchor() {
            HashedCertifiedBlock anchor = pending.poll();
            if (anchor != null) {
                log.info("Synchronizing from anchor: {} on: {}", anchor.hash, params.member());
                transitions.bootstrap(anchor);
                return;
            }
        }

        @Override
        public void awaitRegeneration() {
            if (!started.get()) {
                return;
            }
            final HashedCertifiedBlock g = genesis.get();
            if (g != null) {
                return;
            }
            HashedCertifiedBlock anchor = pending.poll();
            if (anchor != null) {
                log.info("Synchronizing from anchor: {} on: {}", anchor.hash, params.member());
                transitions.bootstrap(anchor);
                return;
            }
            log.info("No anchor to synchronize, waiting: {} cycles on: {}", params.synchronizationCycles(),
                     params.member());
            roundScheduler.schedule(AWAIT_REGEN, () -> {
                futureSynchronization.set(null);
                awaitRegeneration();
            }, params.regenerationCycles());
        }

        @Override
        public void awaitSynchronization() {
            if (!started.get()) {
                return;
            }
            HashedCertifiedBlock anchor = pending.poll();
            if (anchor != null) {
                log.info("Synchronizing from anchor: {} on: {}", anchor.hash, params.member());
                transitions.bootstrap(anchor);
                return;
            }
            log.info("No anchor to synchronize, waiting: {} cycles on: {}", params.synchronizationCycles(),
                     params.member());
            roundScheduler.schedule(AWAIT_SYNC, () -> {
                futureSynchronization.set(null);
                final var c = current.get();
                if (c != null) {
                    synchronizationFailed();
                } else {
                    testQuorum();
                    awaitSynchronization();
                }
            }, params.synchronizationCycles());
        }

        @Override
        public void cancelTimer(String timer) {
            roundScheduler.cancel(timer);
        }

        @Override
        public void combine() {
            CHOAM.this.combine();
        }

        @Override
        public void recover(HashedCertifiedBlock anchor) {
            current.set(new Formation());
            CHOAM.this.recover(anchor);
        }

        @Override
        public void regenerate() {
            current.get().regenerate();
        }

        private void synchronizationFailed() {
            var c = current.get();

            if (c == null) {
                c = testQuorum();
            }

            if (c != null && c.isMember()) {
                log.info("Synchronization failed and initial member, regenerating: {} on: {}",
                         c.getClass().getSimpleName(), params.member());
                transitions.regenerate();
            } else {
                log.info("Synchronization failed, no anchor to recover from: {} on: {}",
                         c == null ? "no formation" : c.getClass().getSimpleName(), params.member());
                transitions.synchronizationFailed();
            }
        }
    }

    public class TransSubmission implements Submitter {
        @Override
        public SubmitResult submit(SubmitTransaction request, Digest from) {
            return CHOAM.this.submit(request, from);
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
        public Initial sync(Synchronize request, Digest from) {
            return CHOAM.this.sync(request, from);
        }
    }

    @FunctionalInterface
    public interface TransactionExecutor {
        default void beginBlock(ULong height, Digest hash) {
        }

        default void endBlock(ULong height, Digest hash) {
        }

        @SuppressWarnings("rawtypes")
        void execute(int index, Digest hash, Transaction tx, CompletableFuture onComplete);

        default void genesis(Digest hash, List<Transaction> initialization) {
        }
    }

    record nextView(ViewMember member, KeyPair consensusKeyPair) {}

    /** abstract class to maintain the common state */
    private abstract class Administration implements Committee {
        protected final Digest viewId;

        private final GroupIterator         servers;
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
            if (!checkJoin(request, from)) {
                log.debug("Join requested for invalid view: {} from: {} on: {}", Digest.from(request.getNextView()),
                          from, params.member());
                return ViewMember.getDefaultInstance();
            }
            final var c = next.get();
            if (log.isDebugEnabled()) {
                log.debug("Joining view: {} from: {} view member: {} on: {}", Digest.from(request.getNextView()), from,
                          ViewContext.print(c.member, params.digestAlgorithm()), params.member());
            }
            return c.member;
        }

        @Override
        public Logger log() {
            return log;
        }

        @Override
        public Parameters params() {
            return params;
        }

        @Override
        public SubmitResult submitTxn(Transaction transaction) {
            Member target = servers.next();
            try (var link = submissionComm.apply(target, params.member())) {
                if (link == null) {
                    log.debug("No link for: {} for submitting txn on: {}", target.getId(), params.member());
                    return SubmitResult.newBuilder().setResult(Result.UNAVAILABLE).build();
                }
//                if (log.isTraceEnabled()) {
//                    log.trace("Submitting received txn: {} to: {} in: {} on: {}",
//                              hashOf(transaction, params.digestAlgorithm()), target.getId(), viewId, params.member());
//                }
                return link.submit(SubmitTransaction.newBuilder()
                                                    .setContext(params.context().getId().toDigeste())
                                                    .setTransaction(transaction)
                                                    .build());
            } catch (StatusRuntimeException e) {
                log.trace("Failed submitting txn: {} status:{} to: {} in: {} on: {}",
                          hashOf(transaction, params.digestAlgorithm()), e.getStatus(), target.getId(), viewId,
                          params.member());
                return SubmitResult.newBuilder()
                                   .setResult(Result.ERROR_SUBMITTING)
                                   .setErrorMsg(e.getStatus().toString())
                                   .build();
            } catch (Throwable e) {
                log.debug("Failed submitting txn: {} to: {} in: {} on: {}",
                          hashOf(transaction, params.digestAlgorithm()), target.getId(), viewId, params.member(), e);
                return SubmitResult.newBuilder().setResult(Result.ERROR_SUBMITTING).setErrorMsg(e.toString()).build();
            }
        }

        @Override
        public boolean validate(HashedCertifiedBlock hb) {
            return validate(hb, validators);
        }
    }

    /** a member of the current committee */
    private class Associate extends Administration {

        private final Producer    producer;
        private final ViewContext viewContext;

        Associate(HashedCertifiedBlock viewChange, Map<Member, Verifier> validators, nextView nextView) {
            super(validators,
                  new Digest(viewChange.block.hasGenesis() ? viewChange.block.getGenesis().getInitialView().getId()
                                                           : viewChange.block.getReconfigure().getId()));
            var context = Committee.viewFor(viewId, params.context());
            context.allMembers().filter(m -> !validators.containsKey(m)).forEach(m -> context.offline(m));
            validators.keySet().forEach(m -> context.activate(m));
            log.trace("Using consensus key: {} sig: {} for view: {} on: {}",
                      params.digestAlgorithm().digest(nextView.consensusKeyPair.getPublic().getEncoded()),
                      params.digestAlgorithm().digest(nextView.member.getSignature().toByteString()), viewId,
                      params.member());
            Signer signer = new SignerImpl(nextView.consensusKeyPair.getPrivate());
            viewContext = new ViewContext(context, params, signer, validators, constructBlock());
            producer = new Producer(viewContext, head.get(), checkpoint.get(), comm);
            producer.start();
        }

        @Override
        public void assembled() {
            producer.assembled();
        }

        @Override
        public void complete() {
            producer.stop();
        }

        @Override
        public SubmitResult submit(SubmitTransaction request) {
//            log.trace("Submit txn: {} to producer on: {}", hashOf(request.getTransaction(), params.digestAlgorithm()),
//                      params().member());
            return producer.submit(request.getTransaction());
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
        private final GenesisAssembly assembly;
        private final Context<Member> formation;

        private Formation() {
            formation = Committee.viewFor(params.genesisViewId(), params.context());
            if (formation.isActive(params.member())) {
                final var c = next.get();
                log.trace("Using genesis consensus key: {} sig: {} on: {}",
                          params.digestAlgorithm().digest(c.consensusKeyPair.getPublic().getEncoded()),
                          params.digestAlgorithm().digest(c.member.getSignature().toByteString()), params.member());
                Signer signer = new SignerImpl(c.consensusKeyPair.getPrivate());
                ViewContext vc = new GenesisContext(formation, params, signer, constructBlock());
                assembly = new GenesisAssembly(vc, comm, next.get().member);
                nextViewId.set(params.genesisViewId());
            } else {
                assembly = null;
            }
        }

        @Override
        public void accept(HashedCertifiedBlock hb) {
            assert hb.height().equals(ULong.valueOf(0));
            final var c = head.get();
            genesis.set(c);
            checkpoint.set(c);
            view.set(c);
            process();
        }

        @Override
        public void complete() {
            if (assembly != null) {
                assembly.stop();
            }
        }

        @Override
        public boolean isMember() {
            return formation.isActive(params.member());
        }

        @Override
        public ViewMember join(JoinRequest request, Digest from) {
            if (!checkJoin(request, from)) {
                return ViewMember.getDefaultInstance();
            }
            final var c = next.get();
            if (log.isDebugEnabled()) {
                log.debug("Joining view: {} from: {} view member: {} on: {}", Digest.from(request.getNextView()), from,
                          ViewContext.print(c.member, params.digestAlgorithm()), params.member());
            }
            return c.member;
        }

        @Override
        public Logger log() {
            return log;
        }

        @Override
        public Parameters params() {
            return params;
        }

        @Override
        public void regenerate() {
            if (assembly != null) {
                assembly.start();
            }
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

    public static Checkpoint checkpoint(DigestAlgorithm algo, File state, int blockSize) {
        Digest stateHash = algo.getOrigin();
        long length = 0;
        if (state != null) {
            try (FileInputStream fis = new FileInputStream(state)) {
                stateHash = algo.digest(fis);
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
            length = state.length();
        }
        Checkpoint.Builder builder = Checkpoint.newBuilder()
                                               .setByteSize(length)
                                               .setSegmentSize(blockSize)
                                               .setStateHash(stateHash.toDigeste());
        log.warn("Checkpoint length: {} segment size: {} stateHash: {}", length, blockSize, stateHash);
        if (state != null) {
            byte[] buff = new byte[blockSize];
            try (FileInputStream fis = new FileInputStream(state)) {
                for (int read = fis.read(buff); read > 0; read = fis.read(buff)) {
                    ByteString segment = ByteString.copyFrom(buff, 0, read);
                    builder.addSegments(algo.digest(segment).toDigeste());
                }
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
        }
        return builder.build();
    }

    public static Block genesis(Digest id, Map<Member, Join> joins, HashedBlock head, Context<Member> context,
                                HashedBlock lastViewChange, Parameters params, HashedBlock lastCheckpoint,
                                Iterable<Transaction> initialization) {
        var reconfigure = reconfigure(id, joins, context, params, params.checkpointBlockSize());
        return Block.newBuilder()
                    .setHeader(buildHeader(params.digestAlgorithm(), reconfigure, head.hash, ULong.valueOf(0),
                                           lastCheckpoint.height(), lastCheckpoint.hash, lastViewChange.height(),
                                           lastViewChange.hash))
                    .setGenesis(Genesis.newBuilder().setInitialView(reconfigure).addAllInitialize(initialization))
                    .build();
    }

    public static Digest hashOf(Transaction transaction, DigestAlgorithm digestAlgorithm) {
        return JohnHancock.from(transaction.getSignature()).toDigest(digestAlgorithm);
    }

    public static Reconfigure reconfigure(Digest id, Map<Member, Join> joins, Context<Member> context,
                                          Parameters params, int checkpointTarget) {
        var builder = Reconfigure.newBuilder().setCheckpointTarget(checkpointTarget).setId(id.toDigeste());

        // Canonical labeling of the view members for Ethereal
        var remapped = rosterMap(context, joins.keySet());

        remapped.keySet()
                .stream()
                .sorted()
                .map(d -> remapped.get(d))
                .peek(m -> builder.addJoins(joins.get(m)))
                .forEach(m -> builder.addView(joins.get(m).getMember()));

        var reconfigure = builder.build();
        return reconfigure;
    }

    public static Block reconfigure(Digest id, Map<Member, Join> joins, HashedBlock head, Context<Member> context,
                                    HashedBlock lastViewChange, Parameters params, HashedBlock lastCheckpoint) {
        final Block lvc = lastViewChange.block;
        int lastTarget = lvc.hasGenesis() ? lvc.getGenesis().getInitialView().getCheckpointTarget()
                                          : lvc.getReconfigure().getCheckpointTarget();
        int checkpointTarget = lastTarget == 0 ? params.checkpointBlockSize() : lastTarget - 1;
        var reconfigure = reconfigure(id, joins, context, params, checkpointTarget);
        return Block.newBuilder()
                    .setHeader(buildHeader(params.digestAlgorithm(), reconfigure, head.hash, head.height().add(1),
                                           lastCheckpoint.height(), lastCheckpoint.hash, lastViewChange.height(),
                                           lastViewChange.hash))
                    .setReconfigure(reconfigure)
                    .build();
    }

    public static Map<Digest, Member> rosterMap(Context<Member> baseContext, Collection<Member> members) {

        // Canonical labeling of the view members for Ethereal
        var ring0 = baseContext.ring(0);
        return members.stream().collect(Collectors.toMap(m -> ring0.hash(m), m -> m));
    }

    public static List<Transaction> toGenesisData(List<? extends Message> initializationData) {
        return toGenesisData(initializationData, DigestAlgorithm.DEFAULT, SignatureAlgorithm.DEFAULT);
    }

    public static List<Transaction> toGenesisData(List<? extends Message> initializationData,
                                                  DigestAlgorithm digestAlgo, SignatureAlgorithm sigAlgo) {
        var source = digestAlgo.getOrigin();
        SignerImpl signer = new SignerImpl(sigAlgo.generateKeyPair().getPrivate());
        AtomicInteger nonce = new AtomicInteger();
        return initializationData.stream()
                                 .map(m -> (Message) m)
                                 .map(m -> Session.transactionOf(source, nonce.incrementAndGet(), m, signer))
                                 .toList();
    }

    private final Map<ULong, CheckpointState>                           cachedCheckpoints     = new ConcurrentHashMap<>();
    private final AtomicReference<HashedCertifiedBlock>                 checkpoint            = new AtomicReference<>();
    private final ReliableBroadcaster                                   combine;
    private final CommonCommunications<Terminal, Concierge>             comm;
    private final CommonCommunications<TxnSubmission, Submitter>        submissionComm;
    private final AtomicReference<Committee>                            current               = new AtomicReference<>();
    private final ExecutorService                                       executions;
    private final AtomicReference<CompletableFuture<SynchronizedState>> futureBootstrap       = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>>                   futureSynchronization = new AtomicReference<>();
    private final AtomicReference<HashedCertifiedBlock>                 genesis               = new AtomicReference<>();
    private final AtomicReference<HashedCertifiedBlock>                 head                  = new AtomicReference<>();
    private final ExecutorService                                       linear;
    private final AtomicReference<nextView>                             next                  = new AtomicReference<>();
    private final AtomicReference<Digest>                               nextViewId            = new AtomicReference<>();
    private final Parameters                                            params;
    private final PriorityBlockingQueue<HashedCertifiedBlock>           pending               = new PriorityBlockingQueue<>();
    private final RoundScheduler                                        roundScheduler;
    private final Session                                               session;
    private final AtomicBoolean                                         started               = new AtomicBoolean();
    private final Store                                                 store;
    private final AtomicBoolean                                         synchronizing         = new AtomicBoolean(false);
    private final Combine.Transitions                                   transitions;
    private final AtomicReference<HashedCertifiedBlock>                 view                  = new AtomicReference<>();

    public CHOAM(Parameters params) {
        this.store = new Store(params.digestAlgorithm(), params.mvBuilder().build());
        this.params = params;
        executions = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "Executions " + params.member().getId());
            thread.setDaemon(true);
            return thread;
        });
        nextView();
        combine = new ReliableBroadcaster(params.context(), params.member(), params.combine(), params.exec(),
                                          params.communications(),
                                          params.metrics() == null ? null : params.metrics().getCombineMetrics());
        linear = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "Linear " + params.member().getId());
            thread.setDaemon(true);
            return thread;
        });
        combine.registerHandler((ctx, messages) -> {
            try {
                linear.execute(() -> combine(messages));
            } catch (RejectedExecutionException e) {
                // ignore
            }
        });
        head.set(new NullBlock(params.digestAlgorithm()));
        view.set(new NullBlock(params.digestAlgorithm()));
        checkpoint.set(new NullBlock(params.digestAlgorithm()));
        final Trampoline service = new Trampoline();
        comm = params.communications()
                     .create(params.member(), params.context().getId(), service,
                             r -> new TerminalServer(params.communications().getClientIdentityProvider(),
                                                     params.metrics(), r, params.exec()),
                             TerminalClient.getCreate(params.metrics()),
                             Terminal.getLocalLoopback(params.member(), service));
        final Submitter txnSubmissionService = new TransSubmission();
        submissionComm = params.communications()
                               .create(params.member(), params.context().getId(), txnSubmissionService,
                                       r -> new TxnSubmitServer(params.communications().getClientIdentityProvider(),
                                                                params.metrics(), r, params.exec()),
                                       TxnSubmitClient.getCreate(params.metrics()),
                                       TxnSubmission.getLocalLoopback(params.member(), txnSubmissionService));
        var fsm = Fsm.construct(new Combiner(), Combine.Transitions.class, Merchantile.INITIAL, true);
        fsm.setName("CHOAM" + params.member().getId() + params.context().getId());
        transitions = fsm.getTransitions();
        roundScheduler = new RoundScheduler("CHOAM" + params.member().getId() + params.context().getId(),
                                            params.context().timeToLive());
        combine.register(i -> roundScheduler.tick(i));
        session = new Session(params, service());
    }

    public boolean active() {
        final var c = current.get();
        return !(c == null || c instanceof Formation);
    }

    public Context<Member> context() {
        return params.context();
    }

    public Combine.Transitions getCurrentState() {
        return transitions.fsm().getCurrentState();
    }

    public Digest getId() {
        return params.context().getId();
    }

    public Session getSession() {
        return session;
    }

    public Digest getViewId() {
        final var viewChange = view.get();
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
        log.info("CHOAM startup, majority: {} on: {}", params.majority(), params.member());
        combine.start(params.producer().gossipDuration(), params.scheduler());
        transitions.fsm().enterStartState();
        transitions.start();
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        session.cancelAll();
        linear.shutdown();
        executions.shutdown();
        final var c = current.get();
        if (c != null) {
            c.complete();
        }
    }

    private void accept(HashedCertifiedBlock next) {
        head.set(next);
        store.put(next);
        final Committee c = current.get();
        c.accept(next);
        log.info("Accepted block: {} height: {} body: {} on: {}", next.hash, next.height(), next.block.getBodyCase(),
                 params.member());
    }

    private Bootstrapper bootstrapper(HashedCertifiedBlock anchor) {
        return new Bootstrapper(anchor, params, store, comm);
    }

    private void cancelSynchronization() {
        final ScheduledFuture<?> fs = futureSynchronization.get();
        if (fs != null) {
            fs.cancel(true);
            futureSynchronization.set(null);
        }
        final CompletableFuture<SynchronizedState> fb = futureBootstrap.get();
        if (fb != null) {
            fb.cancel(true);
            futureBootstrap.set(null);
        }
    }

    private boolean checkJoin(JoinRequest request, Digest from) {
        Member source = params.context().getActiveMember(from);
        if (source == null) {
            log.debug("Request to join from non member: {} on: {}", from, params.member());
            return false;
        }
        Digest nextView = new Digest(request.getNextView());
        final var nextId = nextViewId.get();
        if (nextId == null) {
            log.debug("Cannot join view: {} from: {}, next view has not been defined on: {}", nextView, source,
                      params.member());
            return false;
        }
        if (!nextId.equals(nextView)) {
            log.debug("Request to join incorrect view: {} expected: {} from: {} on: {}", nextView, nextId, source,
                      params.member());
            return false;
        }
        final Set<Member> members = Committee.viewMembersOf(nextView, params.context());
        if (!members.contains(params.member())) {
            log.debug("Not a member of view: {} invalid join request from: {} members: {} on: {}", nextView, source,
                      members, params.member());
            return false;
        }
        return true;
    }

    private Block checkpoint() {
        transitions.beginCheckpoint();
        HashedBlock lb = head.get();
        File state = params.checkpointer().apply(lb.height());
        if (state == null) {
            log.error("Cannot create checkpoint on: {}", params.member());
            transitions.fail();
            return null;
        }
        Checkpoint cp = checkpoint(params.digestAlgorithm(), state, params.checkpointBlockSize());
        if (cp == null) {
            transitions.fail();
            return null;
        }

        final HashedCertifiedBlock v = view.get();
        final HashedBlock c = checkpoint.get();
        final Block block = Block.newBuilder()
                                 .setHeader(buildHeader(params.digestAlgorithm(), cp, lb.hash, lb.height().add(1),
                                                        c.height(), c.hash, v.height(), v.hash))
                                 .setCheckpoint(cp)
                                 .build();

        HashedBlock hb = new HashedBlock(params.digestAlgorithm(), block);
        MVMap<Integer, byte[]> stored = store.putCheckpoint(height(block), state, cp);
        state.delete();
        cachedCheckpoints.put(hb.height(), new CheckpointState(cp, stored));
        log.info("Created checkpoint: {} height: {} on: {}", hb.hash, hb.height(), params.member());
        transitions.finishCheckpoint();
        return block;
    }

    private void combine() {
        var next = pending.peek();
        log.trace("Attempting to combine blocks, peek: {} height: {}, head: {} height: {} on: {}",
                  next == null ? "<null>" : next.hash, next == null ? "-1" : next.height(), head.get().hash,
                  head.get().height(), params.member());
        while (next != null) {
            final HashedCertifiedBlock h = head.get();
            if (h.height() != null && next.height().compareTo(h.height()) <= 0) {
//                log.trace("Have already advanced beyond block: {} height: {} current: {} on: {}", next.hash,
//                          next.height(), h.height(), params.member());
                pending.poll();
            } else if (isNext(next)) {
                if (current.get().validate(next)) {
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

        log.trace("Finished combined, head: {} height: {} on: {}", head.get().hash, head.get().height(),
                  params.member());
    }

    private void combine(List<Msg> messages) {
        messages.forEach(m -> combine(m));
        transitions.combine();
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
        return new BlockProducer() {

            @Override
            public Block checkpoint() {
                return CHOAM.this.checkpoint();
            }

            @Override
            public Block genesis(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous) {
                final HashedCertifiedBlock cp = checkpoint.get();
                final HashedCertifiedBlock v = view.get();
                return CHOAM.genesis(nextViewId, joining, previous, params.context(), v, params, cp,
                                     params.genesisData().apply(joining));
            }

            @Override
            public Block produce(ULong height, Digest prev, Assemble assemble, HashedBlock checkpoint) {
                final HashedCertifiedBlock v = view.get();
                return Block.newBuilder()
                            .setHeader(buildHeader(params.digestAlgorithm(), assemble, prev, height,
                                                   checkpoint.height(), checkpoint.hash, v.height(), v.hash))
                            .setAssemble(assemble)
                            .build();
            }

            @Override
            public Block produce(ULong height, Digest prev, Executions executions, HashedBlock checkpoint) {
                final HashedCertifiedBlock v = view.get();
                return Block.newBuilder()
                            .setHeader(buildHeader(params.digestAlgorithm(), executions, prev, height,
                                                   checkpoint.height(), checkpoint.hash, v.height(), v.hash))
                            .setExecutions(executions)
                            .build();
            }

            @Override
            public void publish(CertifiedBlock cb) {
                combine.publish(cb, true);
                log.trace("Published block height: {} on: {}", cb.getBlock().getHeader().getHeight(), params.member());
            }

            @Override
            public Block reconfigure(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous,
                                     HashedBlock checkpoint) {
                final HashedCertifiedBlock v = view.get();
                return CHOAM.reconfigure(nextViewId, joining, previous, params.context(), v, params, checkpoint);
            }
        };
    }

    private void execute(List<Transaction> execs) {
        final var h = head.get();
        log.info("Executing transactions for block: {} height: {} txns: {} on: {}", h.hash, h.height(), execs.size(),
                 params.member());
        for (int i = 0; i < execs.size(); i++) {
            var exec = execs.get(i);
            Digest hash = hashOf(exec, params.digestAlgorithm());
            var stxn = session.complete(hash);
            try {

                params.processor()
                      .execute(i, CHOAM.hashOf(exec, params.digestAlgorithm()), exec,
                               stxn == null ? null : stxn.onCompletion());
            } catch (Throwable t) {
                log.error("Exception processing transaction: {} block: {} height: {} on: {}", hash, h.hash, h.height(),
                          params.member());
            }
        }
    }

    private CheckpointSegments fetch(CheckpointReplication request, Digest from) {
        Member member = params.context().getMember(from);
        if (member == null) {
            log.warn("Received checkpoint fetch from non member: {} on: {}", from, params.member());
            return CheckpointSegments.getDefaultInstance();
        }
        CheckpointState state = cachedCheckpoints.get(ULong.valueOf(request.getCheckpoint()));
        if (state == null) {
            log.info("No cached checkpoint for {} on: {}", request.getCheckpoint(), params.member());
            return CheckpointSegments.getDefaultInstance();
        }
        CheckpointSegments.Builder replication = CheckpointSegments.newBuilder();

        return replication.addAllSegments(state.fetchSegments(BloomFilter.from(request.getCheckpointSegments()),
                                                              params.maxCheckpointSegments()))
                          .build();
    }

    private Blocks fetchBlocks(BlockReplication rep, Digest from) {
        Member member = params.context().getMember(from);
        if (member == null) {
            log.warn("Received fetchBlocks from non member: {} on: {}", from, params.member());
            return Blocks.getDefaultInstance();
        }
        BloomFilter<ULong> bff = BloomFilter.from(rep.getBlocksBff());
        Blocks.Builder blocks = Blocks.newBuilder();
        store.fetchBlocks(bff, blocks, 5, ULong.valueOf(rep.getFrom()), ULong.valueOf(rep.getTo()));
        return blocks.build();
    }

    private Blocks fetchViewChain(BlockReplication rep, Digest from) {
        Member member = params.context().getMember(from);
        if (member == null) {
            log.warn("Received fetchViewChain from non member: {} on: {}", from, params.member());
            return Blocks.getDefaultInstance();
        }
        BloomFilter<ULong> bff = BloomFilter.from(rep.getBlocksBff());
        Blocks.Builder blocks = Blocks.newBuilder();
        store.fetchViewChain(bff, blocks, 1, ULong.valueOf(rep.getFrom()), ULong.valueOf(rep.getTo()));
        return blocks.build();
    }

    private void genesisInitialization(final HashedBlock h, final List<Transaction> initialization) {
        log.info("Executing genesis initialization block: {} on: {}", h.hash, params.member());
        try {
            params.processor().genesis(h.hash, initialization);
        } catch (Throwable t) {
            log.error("Exception processing genesis initialization block: {} on: {}", h.hash, params.member(), t);
        }
    }

    private boolean isNext(HashedBlock next) {
        if (next == null) {
            return false;
        }
        final var h = head.get();
        if (h.height() == null && next.height().equals(ULong.valueOf(0))) {
            return true;
        }
        final Digest prev = next.getPrevious();
        if (h.hash.equals(prev)) {
            return true;
        }
        return false;
    }

    private ViewMember join(JoinRequest request, Digest from) {
        final var c = current.get();
        if (c == null) {
            return ViewMember.getDefaultInstance();
        }
        return c.join(request, from);
    }

    private void nextView() {
        KeyPair keyPair = params.viewSigAlgorithm().generateKeyPair();
        PubKey pubKey = bs(keyPair.getPublic());
        JohnHancock signed = params.member().sign(pubKey.toByteString());
        if (signed == null) {
            log.error("Unable to generate and sign consensus key on: {}", params.member());
            return;
        }
        log.trace("Generated next view consensus key: {} sig: {} on: {}",
                  params.digestAlgorithm().digest(pubKey.getEncoded()),
                  params.digestAlgorithm().digest(signed.toSig().toByteString()), params.member());
        next.set(new nextView(ViewMember.newBuilder()
                                        .setId(params.member().getId().toDigeste())
                                        .setConsensusKey(pubKey)
                                        .setSignature(signed.toSig())
                                        .build(),
                              keyPair));
    }

    private void process() {
        final HashedCertifiedBlock h = head.get();
        switch (h.block.getBodyCase()) {
        case ASSEMBLE: {
            params.processor().beginBlock(h.height(), h.hash);
            nextViewId.set(Digest.from(h.block.getAssemble().getNextView()));
            log.info("Next view id: {} on: {}", nextViewId.get(), params.member());
            final var c = current.get();
            if (c != null) {
                c.assembled();
            }
            break;
        }
        case RECONFIGURE: {
            params.processor().beginBlock(h.height(), h.hash);
            reconfigure(h.block.getReconfigure());
            break;
        }
        case GENESIS: {
            cancelSynchronization();
            transitions.regenerated();
            genesisInitialization(h, h.block.getGenesis().getInitializeList());
            reconfigure(h.block.getGenesis().getInitialView());
            break;
        }
        case EXECUTIONS: {
            params.processor().beginBlock(h.height(), h.hash);
            execute(h.block.getExecutions().getExecutionsList());
            break;
        }
        case CHECKPOINT: {
            params.processor().beginBlock(h.height(), h.hash);
            var lastCheckpoint = checkpoint.get().height();
            checkpoint.set(h);
            store.gcFrom(h.height(), lastCheckpoint.add(1));
        }
        default:
            break;
        }
        params.processor().endBlock(h.height(), h.hash);
        log.info("End block: {} height: {} on: {}", h.hash, h.height(), params.member());
    }

    private void reconfigure(Reconfigure reconfigure) {
        nextViewId.set(null);
        final Committee c = current.get();
        c.complete();
        var validators = validatorsOf(reconfigure, params.context());
        final var currentView = next.get();
        nextView();
        final HashedCertifiedBlock h = head.get();
        view.set(h);
        if (validators.containsKey(params.member())) {
            current.set(new Associate(h, validators, currentView));
        } else {
            current.set(new Client(validators, getViewId()));
        }
        log.info("Reconfigured to view: {} validators: {} on: {}", new Digest(reconfigure.getId()),
                 validators.entrySet()
                           .stream()
                           .map(e -> String.format("id: %s key: %s", e.getKey(),
                                                   params.digestAlgorithm().digest(e.toString())))
                           .toList(),
                 params.member());
    }

    private void recover(HashedCertifiedBlock anchor) {
        if (futureSynchronization != null) {
            final var c = futureSynchronization.get();
            if (c != null) {
                c.cancel(true);
            }
            futureSynchronization.set(null);
        }
        futureBootstrap.set(bootstrapper(anchor).synchronize().whenComplete((s, t) -> {
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
        }).orTimeout(params.synchronizeTimeout().toMillis(), TimeUnit.MILLISECONDS));
    }

    private void restore() throws IllegalStateException {
        HashedCertifiedBlock lastBlock = store.getLastBlock();
        if (lastBlock == null) {
            log.info("No state to restore from on: {}", params.member().getId());
            return;
        }
        genesis.set(new HashedCertifiedBlock(params.digestAlgorithm(), store.getCertifiedBlock(ULong.valueOf(0))));
        head.set(lastBlock);
        Header header = lastBlock.block.getHeader();
        HashedCertifiedBlock lastView = new HashedCertifiedBlock(params.digestAlgorithm(),
                                                                 store.getCertifiedBlock(ULong.valueOf(header.getLastReconfig())));
        Reconfigure reconfigure = lastView.block.getReconfigure();
        view.set(lastView);
        var validators = validatorsOf(reconfigure, params.context());
        current.set(new Synchronizer(validators));
        log.info("Reconfigured to view: {} on: {}", new Digest(reconfigure.getId()), params.member());
        CertifiedBlock lastCheckpoint = store.getCertifiedBlock(ULong.valueOf(header.getLastCheckpoint()));
        if (lastCheckpoint != null) {
            checkpoint.set(new HashedCertifiedBlock(params.digestAlgorithm(), lastCheckpoint));
        }

        log.info("Restored to: {} lastView: {} lastCheckpoint: {} lastBlock: {} on: {}",
                 new HashedCertifiedBlock(params.digestAlgorithm(), store.getCertifiedBlock(ULong.valueOf(0))).hash,
                 lastView.hash,
                 lastCheckpoint == null ? "<missing>"
                                        : new HashedCertifiedBlock(params.digestAlgorithm(), lastCheckpoint).hash,
                 lastBlock.hash, params.member().getId());
    }

    private void restoreFrom(HashedCertifiedBlock block, CheckpointState checkpoint) {
        cachedCheckpoints.put(block.height(), checkpoint);
        params.restorer().accept(block.height(), checkpoint);
        restore();
    }

    private Function<SubmittedTransaction, SubmitResult> service() {
        return stx -> {
//            log.trace("Submitting transaction: {} in service() on: {}", stx.hash(), params.member()); 
            final var c = current.get();
            if (c == null) {
                return SubmitResult.newBuilder().setResult(Result.NO_COMMITTEE).build();
            }
            try {
                return c.submitTxn(stx.transaction());
            } catch (StatusRuntimeException e) {
                return SubmitResult.newBuilder()
                                   .setResult(Result.ERROR_SUBMITTING)
                                   .setErrorMsg(e.getStatus().toString())
                                   .build();
            }
        };
    }

    /**
     * Submit a transaction from a client
     * 
     * @return
     */
    private SubmitResult submit(SubmitTransaction request, Digest from) {
        if (from == null) {
            return SubmitResult.getDefaultInstance();
        }
        if (params.context().getMember(from) == null) {
            log.debug("Invalid transaction submission from non member: {} on: {}", from, params.member());
            return SubmitResult.newBuilder().setResult(Result.INVALID_SUBMIT).build();
        }
        final var c = current.get();
        if (c == null) {
            log.debug("No committee to submit txn from: {} on: {}", from, params.member());
            return SubmitResult.newBuilder().setResult(Result.NO_COMMITTEE).build();
        }
//        log.trace("Submiting received txn: {} from: {} on: {}",
//                  hashOf(request.getTransaction(), params.digestAlgorithm()), from, params.member());
        return c.submit(request);
    }

    private Initial sync(Synchronize request, Digest from) {
        if (from == null) {
            return Initial.getDefaultInstance();
        }
        Member member = params.context().getMember(from);
        if (member == null) {
            log.warn("Received sync from non member: {} on: {}", from, params.member());
            return Initial.getDefaultInstance();
        }
        Initial.Builder initial = Initial.newBuilder();
        final HashedCertifiedBlock g = genesis.get();
        if (g != null) {
            initial.setGenesis(g.certifiedBlock);
            HashedCertifiedBlock cp = checkpoint.get();
            if (cp != null) {
                ULong height = ULong.valueOf(request.getHeight());

                while (cp.height().compareTo(height) > 0) {
                    cp = new HashedCertifiedBlock(params.digestAlgorithm(),
                                                  store.getCertifiedBlock(ULong.valueOf(cp.block.getHeader()
                                                                                                .getLastCheckpoint())));
                }
                final ULong lastReconfig = ULong.valueOf(cp.block.getHeader().getLastReconfig());
                HashedCertifiedBlock lastView = null;
                if (lastReconfig.equals(ULong.valueOf(0))) {
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
            current1 = store.getCertifiedBlock(state.lastCheckpoint.height().add(1));
        }
        while (current1 != null) {
            synchronizedProcess(current1, false);
            current1 = store.getCertifiedBlock(height(current1.getBlock()).add(1));
        }
        synchronizing.set(false);
        log.info("Synchronized, resuming view: {} deferred blocks: {} on: {}",
                 state.lastCheckpoint != null ? state.lastCheckpoint.hash : state.genesis.hash, pending.size(),
                 params.member());
        try {
            linear.execute(() -> combine());
        } catch (RejectedExecutionException e) {
            // ignore
        }
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
        final HashedCertifiedBlock previousBlock = head.get();
        Header header = block.getHeader();
        if (previousBlock != null) {
            Digest prev = digest(header.getPrevious());
            ULong prevHeight = previousBlock.height();
            if (prevHeight == null) {
                if (!hcb.height().equals(ULong.valueOf(0))) {
                    pending.add(hcb);
                    log.debug("Deferring block on {}.  Block: {} height should be {} and block height is {}",
                              params.member(), hcb.hash, 0, header.getHeight());
                    return;
                }
            } else {
                if (hcb.height().compareTo(prevHeight) <= 0) {
                    log.debug("Discarding previously committed block: {} height: {} current height: {} on: {}",
                              hcb.hash, hcb.height(), prevHeight, params.member());
                    return;
                }
                if (!hcb.height().equals(prevHeight.add(1))) {
                    pending.add(hcb);
                    log.debug("Deferring block on {}.  Block: {} height should be {} and block height is {}",
                              params.member(), hcb.hash, previousBlock.height().add(1), header.getHeight());
                    return;
                }
            }
            if (!previousBlock.hash.equals(prev)) {
                log.error("Protocol violation on {}. New block does not refer to current block hash. Should be {} and next block's prev is {}, current height: {} next height: {}",
                          params.member(), previousBlock.hash, prev, prevHeight, hcb.height());
                return;
            }
            final var c = current.get();
            if (!c.validate(hcb)) {
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
            if (!current.get().validateRegeneration(hcb)) {
                log.error("Protocol violation on: {}. Genesis block is not validated {}", params.member(), hcb.hash);
                return;
            }
        }
        pending.add(hcb);
        if (combine) {
            combine();
        }
    }

    private Committee testQuorum() {
        var activeCount = params.context().activeCount();
        log.info("Active count: {} on: {}", activeCount, params.member());
        if (activeCount >= params.context().getRingCount()) {
            var c = new Formation();
            current.set(c);
            return c;
        }
        return null;
    }
}
