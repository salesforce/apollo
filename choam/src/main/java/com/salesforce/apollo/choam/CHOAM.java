/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.base.Function;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.choam.comm.*;
import com.salesforce.apollo.choam.fsm.Combine;
import com.salesforce.apollo.choam.fsm.Combine.Mercantile;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.choam.proto.SubmitResult.Result;
import com.salesforce.apollo.choam.support.*;
import com.salesforce.apollo.choam.support.Bootstrapper.SynchronizedState;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock.NullBlock;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.DelegatedContext;
import com.salesforce.apollo.cryptography.*;
import com.salesforce.apollo.cryptography.Signer.SignerImpl;
import com.salesforce.apollo.cryptography.proto.PubKey;
import com.salesforce.apollo.membership.GroupIterator;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.RoundScheduler;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.MessageAdapter;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.messaging.proto.AgedMessageOrBuilder;
import com.salesforce.apollo.utils.Utils;
import io.grpc.StatusRuntimeException;
import org.h2.mvstore.MVMap;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.salesforce.apollo.choam.Committee.validatorsOf;
import static com.salesforce.apollo.choam.support.HashedBlock.buildHeader;
import static com.salesforce.apollo.choam.support.HashedBlock.height;
import static com.salesforce.apollo.cryptography.QualifiedBase64.bs;
import static com.salesforce.apollo.cryptography.QualifiedBase64.digest;

/**
 * Combine Honnete Ober Advancer Mercantiles.
 *
 * @author hal.hildebrand
 */
public class CHOAM {
    private static final Logger log = LoggerFactory.getLogger(CHOAM.class);

    private final Map<ULong, CheckpointState>                           cachedCheckpoints     = new ConcurrentHashMap<>();
    private final AtomicReference<HashedCertifiedBlock>                 checkpoint            = new AtomicReference<>();
    private final ReliableBroadcaster                                   combine;
    private final CommonCommunications<Terminal, Concierge>             comm;
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
    private final CommonCommunications<TxnSubmission, Submitter>        submissionComm;
    private final Combine.Transitions                                   transitions;
    private final TransSubmission                                       txnSubmission         = new TransSubmission();
    private final AtomicReference<HashedCertifiedBlock>                 view                  = new AtomicReference<>();
    private final AtomicReference<Context<Member>>                      pendingView           = new AtomicReference<>();

    public CHOAM(Parameters params) {
        this.store = new Store(params.digestAlgorithm(), params.mvBuilder().clone().build());
        this.params = params;
        executions = Executors.newVirtualThreadPerTaskExecutor();

        nextView();
        var bContext = new DelegatedContext<>(params.context());
        var adapter = new MessageAdapter(any -> true, (Function<ByteString, Digest>) this::signatureHash,
                                         (Function<ByteString, List<Digest>>) any -> Collections.emptyList(),
                                         (m, any) -> any,
                                         (Function<AgedMessageOrBuilder, ByteString>) AgedMessageOrBuilder::getContent);

        combine = new ReliableBroadcaster(bContext, params.member(), params.combine(), params.communications(),
                                          params.metrics() == null ? null : params.metrics().getCombineMetrics(),
                                          adapter);
        linear = Executors.newSingleThreadExecutor(
        Thread.ofVirtual().name("Linear " + params.member().getId()).factory());
        combine.registerHandler((_, messages) -> {
            try {
                linear.execute(Utils.wrapped(() -> combine(messages), log));
            } catch (RejectedExecutionException e) {
                // ignore
            }
        });
        head.set(new NullBlock(params.digestAlgorithm()));
        view.set(new NullBlock(params.digestAlgorithm()));
        checkpoint.set(new NullBlock(params.digestAlgorithm()));
        final Trampoline service = new Trampoline();
        comm = params.communications()
                     .create(params.member(), params.context().getId(), service, service.getClass().getCanonicalName(),
                             r -> new TerminalServer(params.communications().getClientIdentityProvider(),
                                                     params.metrics(), r), TerminalClient.getCreate(params.metrics()),
                             Terminal.getLocalLoopback(params.member(), service));
        submissionComm = params.communications()
                               .create(params.member(), params.context().getId(), txnSubmission,
                                       txnSubmission.getClass().getCanonicalName(),
                                       r -> new TxnSubmitServer(params.communications().getClientIdentityProvider(),
                                                                params.metrics(), r),
                                       TxnSubmitClient.getCreate(params.metrics()),
                                       TxnSubmission.getLocalLoopback(params.member(), txnSubmission));
        var fsm = Fsm.construct(new Combiner(), Combine.Transitions.class, Mercantile.INITIAL, true);
        fsm.setName("CHOAM%s on: %s".formatted(params.context().getId(), params.member().getId()));
        transitions = fsm.getTransitions();
        roundScheduler = new RoundScheduler("CHOAM" + params.member().getId() + params.context().getId(),
                                            params.context().timeToLive());
        combine.register(i -> roundScheduler.tick());
        session = new Session(params, service());
    }

    public static Checkpoint checkpoint(DigestAlgorithm algo, File state, int segmentSize, Digest initial, int crowns,
                                        Digest id) {
        assert segmentSize > 0 : "segment size must be > 0 : " + segmentSize;
        long length = 0;
        if (state != null) {
            length = state.length();
        }
        int count = (int) (length / segmentSize);
        if (length != 0 && (long) count * segmentSize < length) {
            count++;
        }
        var accumulator = new HexBloom.HexAccumulator(count, crowns, initial);
        Checkpoint.Builder builder = Checkpoint.newBuilder()
                                               .setCount(count)
                                               .setByteSize(length)
                                               .setSegmentSize(segmentSize);

        if (state != null) {
            byte[] buff = new byte[segmentSize];
            try (FileInputStream fis = new FileInputStream(state)) {
                for (int read = fis.read(buff); read > 0; read = fis.read(buff)) {
                    ByteString segment = ByteString.copyFrom(buff, 0, read);
                    accumulator.add(algo.digest(segment));
                }
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
        }
        var crown = accumulator.build();
        log.info("Checkpoint length: {} segment size: {} count: {} crown: {} initial: {} on: {}", length, segmentSize,
                 builder.getCount(), crown, initial, id);
        var cp = builder.setCrown(crown.toHexBloome()).build();

        var deserialized = HexBloom.from(cp.getCrown());
        log.info("Deserialized checkpoint crown: {} initial: {} on: {}", deserialized, initial, id);
        return cp;
    }

    public static Block genesis(Digest id, Map<Member, Join> joins, HashedBlock head, Context<Member> context,
                                HashedBlock lastViewChange, Parameters params, HashedBlock lastCheckpoint,
                                Iterable<Transaction> initialization) {
        var reconfigure = reconfigure(id, joins, context, params.checkpointBlockDelta());
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

    public static String print(Join join, DigestAlgorithm da) {
        return "J[view: " + Digest.from(join.getMember().getVm().getView()) + " member: " + ViewContext.print(
        join.getMember(), da) + "certifications: " + join.getEndorsementsList()
                                                         .stream()
                                                         .map(c -> ViewContext.print(c, da))
                                                         .toList() + "]";
    }

    public static Reconfigure reconfigure(Digest nextViewId, Map<Member, Join> joins, Context<Member> context,
                                          int checkpointTarget) {
        var builder = Reconfigure.newBuilder().setCheckpointTarget(checkpointTarget).setId(nextViewId.toDigeste());

        // Canonical labeling of the view members for Ethereal
        var remapped = rosterMap(context, joins.keySet());

        remapped.keySet().stream().sorted().map(remapped::get).forEach(m -> builder.addJoins(joins.get(m)));

        var reconfigure = builder.build();
        return reconfigure;
    }

    public static Block reconfigure(Digest nextViewId, Map<Member, Join> joins, HashedBlock head,
                                    Context<Member> context, HashedBlock lastViewChange, Parameters params,
                                    HashedBlock lastCheckpoint) {
        final Block lvc = lastViewChange.block;
        int lastTarget = lvc.hasGenesis() ? lvc.getGenesis().getInitialView().getCheckpointTarget()
                                          : lvc.getReconfigure().getCheckpointTarget();
        int checkpointTarget = lastTarget == 0 ? params.checkpointBlockDelta() : lastTarget - 1;
        var reconfigure = reconfigure(nextViewId, joins, context, checkpointTarget);
        return Block.newBuilder()
                    .setHeader(buildHeader(params.digestAlgorithm(), reconfigure, head.hash, head.height().add(1),
                                           lastCheckpoint.height(), lastCheckpoint.hash, lastViewChange.height(),
                                           lastViewChange.hash))
                    .setReconfigure(reconfigure)
                    .build();
    }

    public static Map<Digest, Member> rosterMap(Context<Member> baseContext, Collection<Member> members) {

        // Canonical labeling of the view members for Ethereal
        return members.stream().collect(Collectors.toMap(m -> baseContext.hashFor(m.getId(), 0), m -> m));
    }

    public static List<Transaction> toGenesisData(List<? extends Message> initializationData) {
        return toGenesisData(initializationData, DigestAlgorithm.DEFAULT, SignatureAlgorithm.DEFAULT);
    }

    public static List<Transaction> toGenesisData(List<? extends Message> initializationData,
                                                  DigestAlgorithm digestAlgo, SignatureAlgorithm sigAlgo) {
        var source = digestAlgo.getOrigin();
        SignerImpl signer = new SignerImpl(sigAlgo.generateKeyPair().getPrivate(), ULong.MIN);
        AtomicInteger nonce = new AtomicInteger();
        return initializationData.stream()
                                 .map(m -> (Message) m)
                                 .map(m -> Session.transactionOf(source, nonce.incrementAndGet(), m, signer))
                                 .toList();
    }

    public boolean active() {
        final var c = current.get();
        HashedCertifiedBlock h = head.get();
        return (c != null && h != null && transitions.fsm().getCurrentState() == Mercantile.OPERATIONAL)
        && c instanceof Administration && h.height().compareTo(ULong.valueOf(1)) >= 0;
    }

    public DelegatedContext<Member> context() {
        return params.context();
    }

    public ULong currentHeight() {
        final var c = head.get();
        return c == null ? null : c.height();
    }

    public Combine.Transitions getCurrentState() {
        return transitions.fsm().getCurrentState();
    }

    public Digest getId() {
        return params.member().getId();
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

    public String logState() {
        final var c = current.get();
        HashedCertifiedBlock h = head.get();
        if (c == null) {
            return "No committee on: %s".formatted(params.member().getId());
        }
        if (h.block == null) {
            return "block is null, committee: %s state: %s on: %s  ".formatted(c.getClass().getSimpleName(),
                                                                               transitions.fsm().getCurrentState(),
                                                                               params.member().getId());
        }
        return "block: %s hash: %s height: %s committee: %s state: %s on: %s  ".formatted(h.block.getBodyCase(), h.hash,
                                                                                          h.height(),
                                                                                          c.getClass().getSimpleName(),
                                                                                          transitions.fsm()
                                                                                                     .getCurrentState(),
                                                                                          params.member().getId());
    }

    /**
     * A view change has occurred
     *
     * @param context - the new membership context
     * @param diadem  - the compact HexBloom of the context view
     */
    public void nextView(Context<Member> context, Digest diadem) {
        ((DelegatedContext<Member>) combine.getContext()).setContext(context);
        var c = current.get();
        if (c != null) {
            c.nextView(context);
        } else {
            log.info("Acquiring new view, diadem: {} size: {} on: {}", diadem, context.size(), params.member().getId());
            params.context().setContext(context);
            pendingView.set(null);
        }
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        log.info("CHOAM startup: {} majority: {} on: {}", params.context().getId(), params.majority(),
                 params.member().getId());
        combine.start(params.producer().gossipDuration());
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
        combine.stop();
    }

    private void accept(HashedCertifiedBlock next) {
        head.set(next);
        store.put(next);
        final Committee c = current.get();
        c.accept(next);
        log.info("Accepted block: {} hash: {} height: {} body: {} on: {}", next.block.getBodyCase(), next.hash,
                 next.height(), next.block.getBodyCase(), params.member().getId());
    }

    private void cancelBootstrap() {
        final CompletableFuture<SynchronizedState> fb = futureBootstrap.get();
        if (fb != null) {
            fb.cancel(true);
            futureBootstrap.set(null);
        }
    }

    private void cancelSynchronization() {
        final ScheduledFuture<?> fs = futureSynchronization.get();
        if (fs != null) {
            fs.cancel(true);
            futureSynchronization.set(null);
        }
    }

    private boolean checkJoin(Digest nextView, Digest from) {
        Member source = params.context().getMember(from);
        if (source == null) {
            log.debug("Request to join from non member: {} on: {}", from, params.member().getId());
            return false;
        }
        final var nextId = nextViewId.get();
        if (nextId == null) {
            log.debug("Cannot join view: {} from: {}, next view has not been defined on: {}", nextView, source.getId(),
                      params.member().getId());
            return false;
        }
        if (!nextId.equals(nextView)) {
            log.debug("Request to join incorrect view: {} expected: {} from: {} on: {}", nextView, nextId,
                      source.getId(), params.member().getId());
            return false;
        }
        final Set<Member> members = Committee.viewMembersOf(nextView, pendingView().get());
        if (!members.contains(params.member())) {
            log.debug("Not a member of view: {} invalid join request from: {} members: {} on: {}", nextView,
                      source.getId(), members.stream().map(m -> m.getId()).toList(), params.member().getId());
            return false;
        }
        return true;
    }

    private Block checkpoint() {
        transitions.beginCheckpoint();
        HashedBlock lb = head.get();
        File state = params.checkpointer().apply(lb.height());
        if (state == null) {
            log.error("Cannot create checkpoint on: {}", params.member().getId());
            transitions.fail();
            return null;
        }
        final HashedBlock c = checkpoint.get();
        Checkpoint cp = checkpoint(params.digestAlgorithm(), state, params.checkpointSegmentSize(), c.hash,
                                   params.crowns(), params.member().getId());
        if (cp == null) {
            transitions.fail();
            return null;
        }

        final HashedCertifiedBlock v = view.get();
        final Block block = Block.newBuilder()
                                 .setHeader(
                                 buildHeader(params.digestAlgorithm(), cp, lb.hash, lb.height().add(1), c.height(),
                                             c.hash, v.height(), v.hash))
                                 .setCheckpoint(cp)
                                 .build();

        HashedBlock hb = new HashedBlock(params.digestAlgorithm(), block);
        MVMap<Integer, byte[]> stored = store.putCheckpoint(height(block), state, cp);
        state.delete();
        cachedCheckpoints.put(hb.height(), new CheckpointState(cp, stored));
        log.info("Created checkpoint: {} height: {} on: {}", hb.hash, hb.height(), params.member().getId());
        transitions.finishCheckpoint();
        return block;
    }

    private void combine() {
        var next = pending.peek();
        log.trace("Attempting to combine blocks, peek: {} height: {}, head: {} height: {} on: {}",
                  next == null ? "<null>" : next.hash, next == null ? "-1" : next.height(), head.get().hash,
                  head.get().height(), params.member().getId());
        while (next != null) {
            final HashedCertifiedBlock h = head.get();
            if (h.height() != null && next.height().compareTo(h.height()) <= 0) {
                pending.poll();
            } else if (isNext(next)) {
                if (current.get().validate(next)) {
                    HashedCertifiedBlock nextBlock = pending.poll();
                    if (nextBlock == null) {
                        return;
                    }
                    accept(nextBlock);
                } else {
                    log.debug("Unable to validate block: {} hash: {} height: {} on: {}", next.block.getBodyCase(),
                              next.hash, next.height(), params.member().getId());
                    pending.poll();
                }
            } else {
                log.trace("Premature block: {} : {} height: {} current: {} on: {}", next.block.getBodyCase(), next.hash,
                          next.height(), h.height(), params.member().getId());
                return;
            }
            next = pending.peek();
        }

        log.trace("Finished combined, head: {} height: {} on: {}", head.get().hash, head.get().height(),
                  params.member().getId());
    }

    private void combine(List<Msg> messages) {
        messages.forEach(this::combine);
        transitions.combine();
    }

    private void combine(Msg m) {
        CertifiedBlock block;
        try {
            block = CertifiedBlock.parseFrom(m.content());
        } catch (InvalidProtocolBufferException e) {
            log.debug("unable to parse block content from {} on: {}", m.source(), params.member().getId());
            return;
        }
        HashedCertifiedBlock hcb = new HashedCertifiedBlock(params.digestAlgorithm(), block);
        log.trace("Received block: {} hash: {} height: {} from {} on: {}", hcb.block.getBodyCase(), hcb.hash,
                  hcb.height(), m.source(), params.member().getId());
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
                var g = CHOAM.genesis(nextViewId, joining, previous, params.context(), v, params, cp,
                                      params.genesisData().apply(joining));
                log.info("Create genesis: {} on: {}", nextViewId, params.member().getId());
                return g;
            }

            @Override
            public void onFailure() {
                transitions.fail();
            }

            @Override
            public Block produce(ULong height, Digest prev, Assemble assemble, HashedBlock checkpoint) {
                final HashedCertifiedBlock v = view.get();
                var block = Block.newBuilder()
                                 .setHeader(
                                 buildHeader(params.digestAlgorithm(), assemble, prev, height, checkpoint.height(),
                                             checkpoint.hash, v.height(), v.hash))
                                 .setAssemble(assemble)
                                 .build();
                log.trace("Produced block: {} height: {} on: {}", block.getBodyCase(), block.getHeader().getHeight(),
                          params.member().getId());
                return block;
            }

            @Override
            public Block produce(ULong height, Digest prev, Executions executions, HashedBlock checkpoint) {
                final HashedCertifiedBlock v = view.get();
                var block = Block.newBuilder()
                                 .setHeader(
                                 buildHeader(params.digestAlgorithm(), executions, prev, height, checkpoint.height(),
                                             checkpoint.hash, v.height(), v.hash))
                                 .setExecutions(executions)
                                 .build();
                log.trace("Produced block: {} height: {} on: {}", block.getBodyCase(), block.getHeader().getHeight(),
                          params.member().getId());
                return block;
            }

            @Override
            public void publish(Digest hash, CertifiedBlock cb) {
                log.info("Publishing: {} hash: {} height: {} certifications: {} on: {}", cb.getBlock().getBodyCase(),
                         hash, ULong.valueOf(cb.getBlock().getHeader().getHeight()), cb.getCertificationsCount(),
                         params.member().getId());
                combine.publish(cb, true);
            }

            @Override
            public Block reconfigure(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous,
                                     HashedBlock checkpoint) {
                final HashedCertifiedBlock v = view.get();
                var block = CHOAM.reconfigure(nextViewId, joining, previous, pendingView().get(), v, params,
                                              checkpoint);
                log.trace("Produced block: {} height: {} on: {}", block.getBodyCase(), block.getHeader().getHeight(),
                          params.member().getId());
                return block;
            }
        };
    }

    private void execute(List<Transaction> execs) {
        final var h = head.get();
        log.info("Executing transactions for block: {} hash: {} height: {} txns: {} on: {}", h.block.getBodyCase(),
                 h.hash, h.height(), execs.size(), params.member().getId());
        for (int i = 0; i < execs.size(); i++) {
            var exec = execs.get(i);
            final var index = i;
            Digest hash = hashOf(exec, params.digestAlgorithm());
            var stxn = session.complete(hash);
            try {
                params.processor()
                      .execute(index, CHOAM.hashOf(exec, params.digestAlgorithm()), exec,
                               stxn == null ? null : stxn.onCompletion(), executions);
            } catch (Throwable t) {
                log.error("Exception processing transaction: {} block: {} height: {} on: {}", hash, h.hash, h.height(),
                          params.member().getId());
            }
        }
    }

    private CheckpointSegments fetch(CheckpointReplication request, Digest from) {
        CheckpointState state = cachedCheckpoints.get(ULong.valueOf(request.getCheckpoint()));
        if (state == null) {
            log.info("No cached checkpoint for {} on: {}", request.getCheckpoint(), params.member().getId());
            return CheckpointSegments.getDefaultInstance();
        }

        return CheckpointSegments.newBuilder()
                                 .addAllSegments(state.fetchSegments(BloomFilter.from(request.getCheckpointSegments()),
                                                                     params.maxCheckpointSegments()))
                                 .build();
    }

    private Blocks fetchBlocks(BlockReplication rep, Digest from) {
        BloomFilter<ULong> bff = BloomFilter.from(rep.getBlocksBff());
        Blocks.Builder blocks = Blocks.newBuilder();
        store.fetchBlocks(bff, blocks, 100, ULong.valueOf(rep.getFrom()), ULong.valueOf(rep.getTo()));
        return blocks.build();
    }

    private Blocks fetchViewChain(BlockReplication rep, Digest from) {
        BloomFilter<ULong> bff = BloomFilter.from(rep.getBlocksBff());
        Blocks.Builder blocks = Blocks.newBuilder();
        store.fetchViewChain(bff, blocks, 100, ULong.valueOf(rep.getFrom()), ULong.valueOf(rep.getTo()));
        return blocks.build();
    }

    private void genesisInitialization(final HashedBlock h, final List<Transaction> initialization) {
        log.info("Executing genesis initialization block: {} on: {}", h.hash, params.member().getId());
        try {
            params.processor().genesis(h.hash, initialization);
        } catch (Throwable t) {
            log.error("Exception processing genesis initialization block: {} on: {}", h.hash, params.member().getId(),
                      t);
        }
    }

    private String getLabel() {
        return "CHOAM" + params.member().getId() + params.context().getId();
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
        var isNext = h.hash.equals(prev);
        if (!isNext) {
            log.info("isNext: false previous: {} block: {} hash: {} height: {} current: {} height: {} on: {}", prev,
                     next.block.getBodyCase(), next.hash, next.height(), h.hash, h.height(), params.member().getId());
        }
        return isNext;
    }

    private SignedViewMember join(Digest nextView, Digest from) {
        final var c = current.get();
        if (c == null) {
            return SignedViewMember.getDefaultInstance();
        }
        return c.join(nextView, from);
    }

    private void nextView() {
        KeyPair keyPair = params.viewSigAlgorithm().generateKeyPair();
        PubKey pubKey = bs(keyPair.getPublic());
        JohnHancock signed = params.member().sign(pubKey.toByteString());
        if (signed == null) {
            log.error("Unable to generate and sign consensus key on: {}", params.member().getId());
            return;
        }
        var committee = current.get();
        log.trace("Generated next view consensus key: {} sig: {} committee: {} on: {}",
                  params.digestAlgorithm().digest(pubKey.getEncoded()),
                  params.digestAlgorithm().digest(signed.toSig().toByteString()),
                  committee == null ? "<no formation>" : committee.getClass().getSimpleName(), params.member().getId());
        next.set(new nextView(ViewMember.newBuilder()
                                        .setId(params.member().getId().toDigeste())
                                        .setConsensusKey(pubKey)
                                        .setSignature(signed.toSig())
                                        .build(), keyPair));
    }

    private Supplier<Context<Member>> pendingView() {
        return () -> {
            var v = pendingView.get();
            return v == null ? params.context() : v;
        };
    }

    private void process() {
        final var c = current.get();
        final HashedCertifiedBlock h = head.get();
        log.info("Begin block: {} hash: {} height: {} committee: {} on: {}", h.block.getBodyCase(), h.hash, h.height(),
                 c.getClass().getSimpleName(), params.member().getId());
        switch (h.block.getBodyCase()) {
        case ASSEMBLE: {
            params.processor().beginBlock(h.height(), h.hash);
            nextViewId.set(Digest.from(h.block.getAssemble().getNextView()));
            log.info("Assembled next view id: {} on: {}", nextViewId.get(), params.member().getId());
            c.assembled();
            break;
        }
        case RECONFIGURE: {
            params.processor().beginBlock(h.height(), h.hash);
            reconfigure(h.block.getReconfigure());
            break;
        }
        case GENESIS: {
            cancelSynchronization();
            cancelBootstrap();
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
        log.info("End block: {} hash: {} height: {} on: {}", h.block.getBodyCase(), h.hash, h.height(),
                 params.member().getId());
    }

    private void reconfigure(Reconfigure reconfigure) {
        log.info("Clearing next view id on: {}", params.member().getId());
        nextViewId.set(null);
        var pv = pendingView.getAndSet(null);
        if (pv != null) {
            // always advance view.
            params.context().setContext(pv);
        }
        final Committee c = current.get();
        c.complete();
        var validators = validatorsOf(reconfigure, params.context());
        final var currentView = next.get();
        nextView();
        final HashedCertifiedBlock h = head.get();
        view.set(h);
        session.setView(h);
        if (validators.containsKey(params.member())) {
            try {
                current.set(new Associate(h, validators, currentView));
            } catch (IllegalArgumentException e) {
                current.set(new Client(validators, getViewId()));
                log.debug("unable to create consensus: {} defaulting to committee: {} on: {}", e.getMessage(),
                          current.get().getClass().getSimpleName(), params.member().getId());
            }
        } else {
            current.set(new Client(validators, getViewId()));
        }
        log.info("Reconfigured to view: {} committee: {} validators: {} on: {}", new Digest(reconfigure.getId()),
                 current.get().getClass().getSimpleName(), validators.entrySet()
                                                                     .stream()
                                                                     .map(e -> String.format("id: %s key: %s",
                                                                                             e.getKey().getId(),
                                                                                             params.digestAlgorithm()
                                                                                                   .digest(
                                                                                                   e.toString())))
                                                                     .toList(), params.member().getId());
    }

    private void recover(HashedCertifiedBlock anchor) {
        cancelBootstrap();
        log.info("Recovering from: {} height: {} on: {}", anchor.hash, anchor.height(), params.member().getId());
        cancelSynchronization();
        cancelBootstrap();
        futureBootstrap.set(new Bootstrapper(anchor, params, store, comm).synchronize());
        futureBootstrap.get().whenComplete((s, t) -> {
            if (t == null) {
                try {
                    synchronize(s);
                } catch (Throwable e) {
                    log.error("Cannot synchronize on: {}", params.member().getId(), e);
                    transitions.fail();
                }
            } else {
                log.error("Synchronization failed on: {}", params.member().getId(), t);
                transitions.fail();
            }
        });
    }

    private void restore() throws IllegalStateException {
        HashedCertifiedBlock lastBlock = store.getLastBlock();
        if (lastBlock == null) {
            log.info("No state to restore from on: {}", params.member().getId());
            return;
        }
        HashedCertifiedBlock geni = new HashedCertifiedBlock(params.digestAlgorithm(),
                                                             store.getCertifiedBlock(ULong.valueOf(0)));
        genesis.set(geni);
        head.set(geni);
        checkpoint.set(geni);
        CertifiedBlock lastCheckpoint = store.getCertifiedBlock(
        ULong.valueOf(lastBlock.block.getHeader().getLastCheckpoint()));
        if (lastCheckpoint != null) {
            HashedCertifiedBlock ckpt = new HashedCertifiedBlock(params.digestAlgorithm(), lastCheckpoint);
            checkpoint.set(ckpt);
            head.set(ckpt);
            HashedCertifiedBlock lastView = new HashedCertifiedBlock(params.digestAlgorithm(), store.getCertifiedBlock(
            ULong.valueOf(ckpt.block.getHeader().getLastReconfig())));
            Reconfigure reconfigure = lastView.block.hasGenesis() ? lastView.block.getGenesis().getInitialView()
                                                                  : lastView.block.getReconfigure();
            view.set(lastView);
            var validators = validatorsOf(reconfigure, params.context());
            current.set(new Synchronizer(validators));
            log.info("Reconfigured to checkpoint view: {} on: {}", new Digest(reconfigure.getId()),
                     params.member().getId());
        }

        log.info("Restored to: {} lastView: {} lastCheckpoint: {} lastBlock: {} on: {}", geni.hash, view.get().hash,
                 checkpoint.get().hash, lastBlock.hash, params.member().getId());
    }

    private void restoreFrom(HashedCertifiedBlock block, CheckpointState checkpoint) {
        cachedCheckpoints.put(block.height(), checkpoint);
        params.restorer().accept(block, checkpoint);
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

    private Digest signatureHash(ByteString any) {
        CertifiedBlock cb;
        try {
            cb = CertifiedBlock.parseFrom(any);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
        return cb.getCertificationsList()
                 .stream()
                 .map(cert -> JohnHancock.from(cert.getSignature()))
                 .map(sig -> sig.toDigest(params.digestAlgorithm()))
                 .reduce(Digest.from(cb.getBlock().getHeader().getBodyHash()), Digest::xor);
    }

    /**
     * Submit a transaction from a client
     *
     * @return
     */
    private SubmitResult submit(Transaction request, Digest from) {
        if (from == null) {
            return SubmitResult.getDefaultInstance();
        }
        if (params.context().getMember(from) == null) {
            log.debug("Invalid transaction submission from non member: {} on: {}", from, params.member().getId());
            return SubmitResult.newBuilder().setResult(Result.INVALID_SUBMIT).build();
        }
        final var c = current.get();
        if (c == null) {
            log.debug("No committee to submit txn from: {} on: {}", from, params.member().getId());
            return SubmitResult.newBuilder().setResult(Result.NO_COMMITTEE).build();
        }
        return c.submit(request);
    }

    private Initial sync(Synchronize request, Digest from) {
        if (from == null) {
            return Initial.getDefaultInstance();
        }
        final HashedCertifiedBlock g = genesis.get();
        if (g != null) {
            Initial.Builder initial = Initial.newBuilder();
            initial.setGenesis(g.certifiedBlock);
            HashedCertifiedBlock cp = checkpoint.get();
            if (cp != null) {
                ULong height = ULong.valueOf(request.getHeight());

                while (cp.height().compareTo(height) > 0) {
                    cp = new HashedCertifiedBlock(params.digestAlgorithm(), store.getCertifiedBlock(
                    ULong.valueOf(cp.block.getHeader().getLastCheckpoint())));
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
                          params.member().getId());
            } else {
                log.debug("Returning sync: {} to: {} on: {}", g.hash, from, params.member().getId());
            }
            return initial.build();
        } else {
            log.debug("Genesis undefined, returning null sync to: {} on: {}", from, params.member().getId());
            return Initial.getDefaultInstance();
        }
    }

    private void synchronize(SynchronizedState state) {
        transitions.synchronizing();
        CertifiedBlock current1;
        if (state.lastCheckpoint() == null) {
            log.info("Synchronizing from genesis: {} on: {}", state.genesis().hash, params.member().getId());
            current1 = state.genesis().certifiedBlock;
        } else {
            log.info("Synchronizing from checkpoint: {} on: {}", state.lastCheckpoint().hash, params.member().getId());
            restoreFrom(state.lastCheckpoint(), state.checkpoint());
            current1 = store.getCertifiedBlock(state.lastCheckpoint().height().add(1));
        }
        while (current1 != null) {
            synchronizedProcess(current1);
            current1 = store.getCertifiedBlock(height(current1.getBlock()).add(1));
        }
        log.info("Synchronized, resuming view: {} deferred blocks: {} on: {}",
                 state.lastCheckpoint() != null ? state.lastCheckpoint().hash : state.genesis().hash, pending.size(),
                 params.member().getId());
        try {
            linear.execute(Utils.wrapped(() -> {
                transitions.regenerated();
                transitions.combine();
            }, log));
        } catch (RejectedExecutionException e) {
            // ignore
        }
    }

    private void synchronizedProcess(CertifiedBlock certifiedBlock) {
        if (!started.get()) {
            log.info("Not started on: {}", params.member().getId());
            return;
        }
        HashedCertifiedBlock hcb = new HashedCertifiedBlock(params.digestAlgorithm(), certifiedBlock);
        Block block = hcb.block;
        log.info("Synchronizing block: {}:{} height: {} on: {}", hcb.hash, block.getBodyCase(), hcb.height(),
                 params.member().getId());
        final HashedCertifiedBlock previousBlock = head.get();
        Header header = block.getHeader();
        if (previousBlock != null) {
            Digest prev = digest(header.getPrevious());
            ULong prevHeight = previousBlock.height();
            if (prevHeight == null) {
                if (!hcb.height().equals(ULong.valueOf(0))) {
                    pending.add(hcb);
                    log.debug("Deferring block: {} hash: {} height should be {} and block height is {} on: {}",
                              hcb.block.getBodyCase(), hcb.hash, 0, header.getHeight(), params.member().getId());
                    return;
                }
            } else {
                if (hcb.height().compareTo(prevHeight) <= 0) {
                    log.trace("Discarding previously committed block: {} height: {} current height: {} on: {}",
                              hcb.hash, hcb.height(), prevHeight, params.member().getId());
                    pending.add(hcb);
                    return;
                }
                if (!hcb.height().equals(prevHeight.add(1))) {
                    pending.add(hcb);
                    log.debug("Deferring block: {} hash: {} height should be {} and block height is {} on: {}",
                              hcb.block.getBodyCase(), hcb.hash, previousBlock.height().add(1), header.getHeight(),
                              params.member().getId());
                    return;
                }
            }
            if (!previousBlock.hash.equals(prev)) {
                log.error(
                "Protocol violation on: {}. New block does not refer to current block hash. Should be: {} and next block's prev is: {}, current height: {} next height: {} on: {}",
                params.member().getId(), previousBlock.hash, prev, prevHeight, hcb.height(), params.member().getId());
                return;
            }
            final var c = current.get();
            if (!c.validate(hcb)) {
                log.error("Protocol violation. New block is not validated: {} hash: {} on: {}", hcb.block.getBodyCase(),
                          hcb.hash, params.member().getId());
                return;
            }
        } else {
            if (!block.hasGenesis()) {
                pending.add(hcb);
                log.info("Deferring block on: {}.  Block: {} hash: {} height should be {} and block height is {}",
                         params.member().getId(), hcb.block.getBodyCase(), hcb.hash, 0, header.getHeight());
                return;
            }
            if (!current.get().validateRegeneration(hcb)) {
                log.error("Protocol violation. Genesis block is not validated: {} hash {} on: {}",
                          hcb.block.getBodyCase(), hcb.hash, params.member().getId());
                return;
            }
        }
        log.info("Deferring block on: {}. Block: {} hash: {} height is {}", params.member().getId(),
                 hcb.block.getBodyCase(), hcb.hash, header.getHeight());
        pending.add(hcb);
    }

    public interface BlockProducer {
        Block checkpoint();

        Block genesis(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous);

        void onFailure();

        Block produce(ULong height, Digest prev, Assemble assemble, HashedBlock checkpoint);

        Block produce(ULong height, Digest prev, Executions executions, HashedBlock checkpoint);

        void publish(Digest hash, CertifiedBlock cb);

        Block reconfigure(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous, HashedBlock checkpoint);
    }

    @FunctionalInterface
    public interface TransactionExecutor {
        default void beginBlock(ULong height, Digest hash) {
        }

        default void endBlock(ULong height, Digest hash) {
        }

        @SuppressWarnings("rawtypes")
        void execute(int index, Digest hash, Transaction tx, CompletableFuture onComplete, Executor executor);

        default void genesis(Digest hash, List<Transaction> initialization) {
        }
    }

    record nextView(ViewMember member, KeyPair consensusKeyPair) {
    }

    public class Combiner implements Combine {

        @Override
        public void anchor() {
            HashedCertifiedBlock anchor = pending.poll();
            var pending = pendingView().get();
            if (anchor != null && pending.totalCount() >= pending.majority()) {
                log.info("Synchronizing from anchor: {} cardinality: {} on: {}", anchor.hash, pending.totalCount(),
                         params.member().getId());
                transitions.bootstrap(anchor);
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
                log.info("Synchronizing from anchor: {} on: {}", anchor.hash, params.member().getId());
                transitions.bootstrap(anchor);
                return;
            }
            log.info("No anchor to synchronize, waiting: {} cycles on: {}", params.synchronizationCycles(),
                     params.member().getId());
            roundScheduler.schedule(AWAIT_REGEN, () -> {
                cancelSynchronization();
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
                log.info("Synchronizing from anchor: {} on: {}", anchor.hash, params.member().getId());
                transitions.bootstrap(anchor);
                return;
            }
            roundScheduler.schedule(AWAIT_SYNC, () -> {
                log.trace("Synchronization failed on: {}", params.member().getId());
                try {
                    synchronizationFailed();
                } catch (IllegalStateException e) {
                    final var c = current.get();
                    log.debug(
                    "Synchronization quorum formation failed: {}, members: {} desired: {} required: {}, no anchor to recover from: {} on: {}",
                    e.getMessage(), context().totalCount(), context().getRingCount(), params.majority(),
                    c == null ? "<no formation>" : c.getClass().getSimpleName(), params.member().getId());
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
            linear.execute(Utils.wrapped(() -> CHOAM.this.combine(), log));
        }

        @Override
        public void fail() {
            log.info("Failed!  Shutting down on: {}", params.member().getId());
            stop();
            params.onFailure().complete(null);
        }

        @Override
        public void recover(HashedCertifiedBlock anchor) {
            log.info("Anchor discovered: {} hash: {} height: {} on: {}", anchor.block.getBodyCase(), anchor.hash,
                     anchor.height(), params.member().getId());
            current.set(new Formation());
            CHOAM.this.recover(anchor);
        }

        @Override
        public void regenerate() {
            current.get().regenerate();
        }

        private void synchronizationFailed() {
            cancelSynchronization();
            var activeCount = context().totalCount();
            var majority = params.majority();
            if (params.generateGenesis() && activeCount >= majority) {
                if (current.get() == null && current.compareAndSet(null, new Formation())) {
                    log.info(
                    "Quorum achieved, triggering regeneration. members: {} required: {} forming Genesis committee on: {}",
                    activeCount, majority, params.member().getId());
                    transitions.regenerate();
                } else {
                    log.info("Quorum achieved, members: {} required: {} existing committee: {} on: {}", activeCount,
                             majority, current.get().getClass().getSimpleName(), params.member().getId());
                }
            } else {
                final var c = current.get();
                log.trace("Synchronization failed; members: {}, no anchor to recover from: {} on: {}", activeCount,
                          c == null ? "<no committee>" : c.getClass().getSimpleName(), params.member().getId());
                awaitSynchronization();
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
        public SignedViewMember join(Digest nextView, Digest from) {
            return CHOAM.this.join(nextView, from);
        }

        @Override
        public Initial sync(Synchronize request, Digest from) {
            return CHOAM.this.sync(request, from);
        }
    }

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
        public SignedViewMember join(Digest nextView, Digest from) {
            if (!checkJoin(nextView, from)) {
                log.debug("Join requested for invalid view: {} from: {} on: {}", nextView, from,
                          params.member().getId());
                return SignedViewMember.getDefaultInstance();
            }
            final var c = next.get();
            var inView = ViewMember.newBuilder(c.member).setView(nextView.toDigeste()).build();

            if (log.isDebugEnabled()) {
                log.debug("Joining view: {} from: {} view member: {} on: {}", nextView, from,
                          ViewContext.print(inView, params.digestAlgorithm()), params.member().getId());
            }
            return SignedViewMember.newBuilder()
                                   .setVm(inView)
                                   .setSignature(params.member().sign(inView.toByteString()).toSig())
                                   .build();
        }

        @Override
        public Logger log() {
            return log;
        }

        @Override
        public void nextView(Context<Member> pendingView) {
            var previous = CHOAM.this.pendingView.getAndSet(pendingView);
            log.info("Pending view: {} size: {} on: {}", nextViewId.get(), pendingView.size(), params.member().getId());
        }

        @Override
        public Parameters params() {
            return params;
        }

        @Override
        public SubmitResult submitTxn(Transaction transaction) {
            if (!servers.hasNext()) {
                log.trace("Failed submitting txn: {} no servers available in: {} on: {}",
                          hashOf(transaction, params.digestAlgorithm()), viewId, params.member().getId());
                return SubmitResult.newBuilder()
                                   .setResult(Result.ERROR_SUBMITTING)
                                   .setErrorMsg("no servers available")
                                   .build();
            }
            Member target = servers.next();
            try (var link = submissionComm.connect(target)) {
                if (link == null) {
                    log.debug("No link for: {} for submitting txn on: {}", target.getId(), params.member().getId());
                    return SubmitResult.newBuilder().setResult(Result.UNAVAILABLE).build();
                }
                return link.submit(transaction);
            } catch (StatusRuntimeException e) {
                log.trace("Failed submitting txn: {} status:{} to: {} in: {} on: {}",
                          hashOf(transaction, params.digestAlgorithm()), e.getStatus(), target.getId(), viewId,
                          params.member().getId());
                return SubmitResult.newBuilder()
                                   .setResult(Result.ERROR_SUBMITTING)
                                   .setErrorMsg(e.getStatus().toString())
                                   .build();
            } catch (Throwable e) {
                log.debug("Failed submitting txn: {} to: {} in: {} on: {}",
                          hashOf(transaction, params.digestAlgorithm()), target.getId(), viewId,
                          params.member().getId(), e);
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

        private final Producer producer;

        Associate(HashedCertifiedBlock viewChange, Map<Member, Verifier> validators, nextView nextView) {
            super(validators, new Digest(
            viewChange.block.hasGenesis() ? viewChange.block.getGenesis().getInitialView().getId()
                                          : viewChange.block.getReconfigure().getId()));
            var context = Committee.viewFor(viewId, params.context());
            log.trace("Using consensus key: {} sig: {} for view: {} on: {}",
                      params.digestAlgorithm().digest(nextView.consensusKeyPair.getPublic().getEncoded()),
                      params.digestAlgorithm().digest(nextView.member.getSignature().toByteString()), viewId,
                      params.member().getId());
            Signer signer = new SignerImpl(nextView.consensusKeyPair.getPrivate(), ULong.MIN);
            Supplier<Context<Member>> pv = pendingView();
            producer = new Producer(new ViewContext(context, params, pv, signer, validators, constructBlock()),
                                    head.get(), checkpoint.get(), comm, getLabel());
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
        public SubmitResult submit(Transaction request) {
            return producer.submit(request);
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
            if (formation.isMember(params.member()) && params.generateGenesis()) {
                final var c = next.get();
                log.trace("Using genesis consensus key: {} sig: {} on: {}",
                          params.digestAlgorithm().digest(c.consensusKeyPair.getPublic().getEncoded()),
                          params.digestAlgorithm().digest(c.member.getSignature().toByteString()),
                          params.member().getId());
                Signer signer = new SignerImpl(c.consensusKeyPair.getPrivate(), ULong.MIN);
                Supplier<Context<Member>> supp = pendingView();
                ViewContext vc = new GenesisContext(formation, supp, params, signer, constructBlock());
                var inView = ViewMember.newBuilder(c.member).setView(params.genesisViewId().toDigeste()).build();
                var svm = SignedViewMember.newBuilder()
                                          .setVm(inView)
                                          .setSignature(params.member().sign(inView.toByteString()).toSig())
                                          .build();
                assembly = new GenesisAssembly(vc, comm, svm, getLabel());
                log.info("Setting next view id to genesis: {} on: {}", params.genesisViewId(), params.member().getId());
                nextViewId.set(params.genesisViewId());
            } else {
                log.trace("No formation on: {}", params.member().getId());
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
            return formation.isMember(params.member());
        }

        @Override
        public SignedViewMember join(Digest nextView, Digest from) {
            if (!checkJoin(nextView, from)) {
                return SignedViewMember.getDefaultInstance();
            }
            final var c = next.get();
            var cd = pendingView().get();
            var inView = ViewMember.newBuilder(c.member).setView(nextView.toDigeste()).build();

            if (log.isDebugEnabled()) {
                log.debug("Joining view: {} from: {} view member: {} on: {}", nextView, from,
                          ViewContext.print(inView, params.digestAlgorithm()), params.member().getId());
            }
            return SignedViewMember.newBuilder()
                                   .setVm(inView)
                                   .setSignature(params.member().sign(inView.toByteString()).toSig())
                                   .build();
        }

        @Override
        public Logger log() {
            return log;
        }

        @Override
        public void nextView(Context<Member> pendingView) {
            log.info("Cancelling formation, acquiring new view, size: {} on: {}", pendingView.size(),
                     params.member().getId());
            params.context().setContext(pendingView);
            CHOAM.this.pendingView.set(null);

            transitions.nextView();
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
                log.debug("Invalid genesis block: {} on: {}", hb.hash, params.member().getId());
                return false;
            }
            return validateRegeneration(hb);
        }
    }

    /** a synchronizer of the current committee */
    private class Synchronizer implements Committee {

        private final Map<Member, Verifier> validators;

        public Synchronizer(Map<Member, Verifier> validators) {
            this.validators = validators;
        }

        @Override
        public void accept(HashedCertifiedBlock next) {
            process();
        }

        @Override
        public void complete() {
        }

        @Override
        public boolean isMember() {
            return false;
        }

        @Override
        public SignedViewMember join(Digest nextView, Digest from) {
            return SignedViewMember.getDefaultInstance();
        }

        @Override
        public Logger log() {
            return log;
        }

        @Override
        public void nextView(Context<Member> pendingView) {
            log.info("Acquiring new view, size: {} on: {}", pendingView.size(), params.member().getId());
            params.context().setContext(pendingView);
            CHOAM.this.pendingView.set(null);
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

    private class TransSubmission implements Submitter {
        @Override
        public SubmitResult submit(Transaction request, Digest from) {
            return CHOAM.this.submit(request, from);
        }
    }
}
