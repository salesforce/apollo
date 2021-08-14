/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.support.HashedBlock.hash;
import static com.salesforce.apollo.choam.support.HashedBlock.height;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.security.PublicKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Join.Builder;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Joins;
import com.salesfoce.apollo.choam.proto.Publish;
import com.salesfoce.apollo.choam.proto.Sync;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.choam.CHOAM.nextView;
import com.salesforce.apollo.choam.fsm.Driven;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;
import com.salesforce.apollo.choam.fsm.Earner;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.utils.Hex;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.SimpleChannel;
import com.salesforce.apollo.utils.Utils;

/**
 * An "Earner"
 * 
 * @author hal.hildebrand
 *
 */
public class Producer {

    /** Leaf action driver coupling for the Producer FSM */
    private class DriveIn implements Driven {
        private final SliceIterator<Terminal>   committee;
        private final Map<Member, Join.Builder> joins        = new ConcurrentHashMap<>();
        private final Set<Member>               nextAssembly = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private volatile int                    principal    = 0;
        private final Set<Member>               syncd        = Collections.newSetFromMap(new ConcurrentHashMap<>());

        DriveIn() {
            Context<? extends Member> context = coordinator.getContext();
            List<? extends Member> slice = context.allMembers().collect(Collectors.toList());
            committee = new SliceIterator<Terminal>("Committe for " + context.getId(), params.member(), slice, comms,
                                                    params.dispatcher());
        }

        @Override
        public void assemble(Joins j) {
            for (Join join : j.getJoinsList()) {
                Digest id = new Digest(join.getView());
                if (!nextViewId.get().equals(id)) {
                    log.debug("Invalid view id: {}  on: {}", id, params.member());
                    continue;
                }
                Digest mid = new Digest(join.getMember().getId());
                Member delegate = params.context().getActiveMember(mid);
                if (delegate == null) {
                    log.debug("Join unknown member: {}  on: {}", mid, params.member());
                    continue;
                }
                Builder proxy = joins.get(delegate);
                if (proxy == null) {
                    log.debug("Join unknown delegate: {}  on: {}", mid, params.member());
                    continue;
                }
                if (!proxy.hasMember()) {
                    proxy.setMember(join.getMember());
                }
                Set<Certification> certs = new HashSet<>(proxy.getEndorsementsList());
                certs.addAll(join.getEndorsementsList());
                proxy.clearEndorsements();
                if (!certs.isEmpty()) {
                    proxy.addAllEndorsements(certs);
                    log.debug("Joins for: {} endorsements: {} on: {}", mid, certs.size(), params.member());
                }
            }
            attemptAssembly();
        }

        @Override
        public void cancelTimer(String label) {
            roundScheduler.cancel(label);
        }

        @Override
        public void complete() {
            Producer.this.complete();
        }

        @Override
        public void convene() {
            conveneThem();
        }

        @Override
        public void epochEnd() {
            final HashedBlock lb = previousBlock.get();
            nextViewId.set(lb.hash);
            reconfiguration.clearBlock().clearCertifications();
            log.info("Consensus complete, next view: {} on: {}", lb.hash, params.member());
        }

        @Override
        public void gatherAssembly() {
            final Digest nv = nextViewId.get();
            coordinator.start(params.gossipDuration(), params.scheduler());
            JoinRequest request = JoinRequest.newBuilder().setContext(params.context().getId().toDigeste())
                                             .setNextView(nv.toDigeste()).build();
            AtomicBoolean proceed = new AtomicBoolean(true);
            AtomicReference<Runnable> reiterate = new AtomicReference<>();
            AtomicInteger countDown = new AtomicInteger(3); // 3 rounds of attempts
            reiterate.set(Utils.wrapped(() -> committee.iterate((term, m) -> {
                final Builder j = joins.get(m);
                return j == null || j.hasMember() ? null : term.join(request);
            }, (futureSailor, term, m) -> consider(futureSailor, term, m, proceed), () -> {
                if (completeSlice()) {
                    proceed.set(false);
                    transitions.assembled();
                } else if (countDown.decrementAndGet() >= 0) {
                    reiterate.get().run();
                } else {
                    transitions.assembled();
                    proceed.set(false);
                }
            }), log));
            reiterate.get().run();
        }

        @Override
        public void published(Publish published) {
            transitions.reconfigured(); // TODO verification
        }

        @Override
        public void reconfigure() {
            if (isPrincipal()) {
                if (reconfiguration.getCertificationsCount() > params.context().toleranceLevel()) {
                    final HashedCertifiedBlock r = new HashedCertifiedBlock(params.digestAlgorithm(),
                                                                            reconfiguration.build());
                    publisher.accept(r);
                    log.debug("Reconfiguring to: {} from: {} block: {} height: {} certs: {} on: {}", nextViewId.get(),
                              getViewId(), r.hash, r.height(), r.certifiedBlock.getCertificationsCount(),
                              params.member());
                    coordinator.publish(Coordinate.newBuilder()
                                                  .setPublish(Publish.newBuilder()
                                                                     .addAllCertifications(reconfiguration.getCertificationsList())
                                                                     .setHeader(reconfiguration.getBlock().getHeader()))
                                                  .build());
                    transitions.reconfigured();
                    return;
                }
                Map<Member, Join> joined = joins.entrySet().stream().filter(e -> e.getValue().hasMember())
                                                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));
                final Digest nv = nextViewId.get();
                Block reconfigure = reconfigureBlock.apply(joined, nv);
                Validate validation = generateValidation(params.digestAlgorithm().digest(reconfigure.toByteString()),
                                                         reconfigure);
                reconfiguration.setBlock(reconfigure).addCertifications(validation.getWitness());
                coordinator.publish(Coordinate.newBuilder().setReconfigure(reconfigure).build().toByteString());
                coordinator.publish(Coordinate.newBuilder().setViewValidate(validation).build().toByteString());
            }
            roundScheduler.schedule(RECONFIGURE, () -> reconfigure(), 1);
        }

        @Override
        public void reconfigure(Block reconfigure) {
            HashedBlock hb = new HashedBlock(params.digestAlgorithm(), reconfigure);
            Validate validation = generateValidation(hb.hash, hb.block);
            if (validation != null) {
                coordinator.publish(Coordinate.newBuilder().setViewValidate(validation).build());
            }
        }

        @Override
        public void startProduction() {
            log.debug("Starting production of: {} on: {}", getViewId(), params.member());
            coordinator.start(params.gossipDuration(), params.scheduler());
            controller.start();
        }

        @Override
        public void sync(Sync sync, Digest from) {
            var member = coordinator.getContext().getActiveMember(from);
            if (member == null) {
                log.trace("Sync received from non member: {} on: {}", from, params.member());
                return;
            }
            syncd.add(member);
            log.trace("Sync received from: {} count: {} on: {}", from, syncd.size(), params.member());
            if (syncd.size() > params.context().toleranceLevel()) {
                transitions.synchd();
            }
        }

        @Override
        public void synchronize() {
            syncd.clear();
            syncd.add(params.member());
            coordinator.start(params.gossipDuration(), params.scheduler());
            nextAssembly.clear();
            var nv = nextViewId.get();
            nextAssembly.addAll(Committee.viewMembersOf(nv, params.context()));
            log.info("Next assembly: {} for: {} on: {}", nextAssembly, nv, params.member());
            nextAssembly.forEach(m -> joins.put(m, Join.newBuilder().setView(nv.toDigeste())));
            attemptSync();

        }

        private void attemptSync() {
            if (syncd.size() > params.context().toleranceLevel()) {
                transitions.synchd();
                return;
            }
            log.debug("Attempting synchronization: {} current: {} on: {}", getViewId(), syncd.size(), params.member());
            coordinator.publish(Coordinate.newBuilder()
                                          .setSync(Sync.newBuilder().setSource(params.member().getId().toDigeste()))
                                          .build());
            roundScheduler.schedule(SYNCHRONIZE, () -> attemptSync(), 1);
        }

        @Override
        public void valdateBlock(Validate validate) {
            var hash = new Digest(validate.getHash());
            if (published.contains(hash)) {
                log.debug("Block: {} already published on: {}", hash, params.member());
                return;
            }
            var p = pending.computeIfAbsent(hash, h -> CertifiedBlock.newBuilder());
            p.addCertifications(validate.getWitness());
            log.trace("Validation for block: {} height: {} on: {}", hash,
                      p.hasBlock() ? height(p.getBlock()) : "missing", params.member());
            maybePublish(hash, p);
        }

        @Override
        public void validation(Validate validate) {
            var hash = new Digest(validate.getHash());
            final Certification witness = validate.getWitness();
            final Digest source = new Digest(witness.getId());

            if (reconfiguration.hasBlock()) {
                final Digest reconHash = params.digestAlgorithm().digest(reconfiguration.getBlock().toByteString());
                if (!hash.equals(reconHash)) {
                    log.trace("Incorrect validation for block: {} height: {} from: {} on: {}", hash,
                              height(reconfiguration.getBlock()), source, params.member());
                    return;
                }
            }
            log.trace("Validation added for block: {} height: {} from: {} on: {}", hash,
                      height(reconfiguration.getBlock()), source, params.member());
            reconfiguration.addCertifications(witness);
        }

        private void attemptAssembly() {
            int toleranceLevel = params.context().toleranceLevel();
            final long crossedThreshold = joins.values().stream().filter(b -> b.getEndorsementsCount() > toleranceLevel)
                                               .count();
            log.debug("Joins for: {} crossed: {} on: {}", nextViewId.get(), crossedThreshold, params.member());
            if (crossedThreshold > toleranceLevel) {
                log.debug("Nominated: {} on: {}", nextViewId.get(), params.member());
                transitions.nominated();
            }
        }

        private boolean completeSlice() {
            return joins.values().stream().filter(j -> j.getMember() == null).count() == joins.size();
        }

        private boolean consider(Optional<ListenableFuture<ViewMember>> futureSailor, Terminal term, Member m,
                                 AtomicBoolean proceed) {

            if (futureSailor.isEmpty()) {
                return true;
            }
            ViewMember member;
            try {
                member = futureSailor.get().get();
                log.debug("Join reply from: {} on: {}", term.getMember().getId(), params.member().getId());
            } catch (InterruptedException e) {
                log.debug("Error join response from: {} on: {}", term.getMember().getId(), params.member().getId(), e);
                return proceed.get();
            } catch (ExecutionException e) {
                log.debug("Error join response from: {} on: {}", term.getMember().getId(), params.member().getId(),
                          e.getCause());
                return proceed.get();
            }
            if (member.equals(ViewMember.getDefaultInstance())) {
                log.debug("Empty join response from: {} on: {}", term.getMember().getId(), params.member().getId());
                return proceed.get();
            }

            PubKey encoded = member.getConsensusKey();

            if (!term.getMember().verify(signature(member.getSignature()), encoded.toByteString())) {
                log.debug("Could not verify consensus key from: {} on: {}", term.getMember().getId(), params.member());
                return proceed.get();
            }
            PublicKey consensusKey = publicKey(encoded);
            if (consensusKey == null) {
                log.debug("Could not deserialize consensus key from: {} on: {}", term.getMember().getId(),
                          params.member());
                return proceed.get();
            }
            JohnHancock signed = params.member().sign(encoded.toByteString());
            if (signed == null) {
                log.debug("Could not sign consensus key from: {} on: {}", term.getMember().getId(), params.member());
                return proceed.get();
            }
            log.debug("Adding delegate to: {} from: {} on: {}", getViewId(), term.getMember().getId(), params.member());
            joins.get(term.getMember()).setMember(member)
                 .addEndorsements(Certification.newBuilder().setId(params.member().getId().toDigeste())
                                               .setSignature(signed.toSig()));
            return proceed.get();
        }

        private void conveneThem() {
            roundScheduler.schedule(Driven.RECONVENE, () -> conveneThem(), 1);
            log.debug("Publishing: {} joins: {} on: {}", getViewId(), joins.size(), params.member());
            coordinator.publish(Coordinate.newBuilder()
                                          .setJoins(Joins.newBuilder()
                                                         .addAllJoins(joins.values().stream().filter(b -> b.hasMember())
                                                                           .map(b -> b.build()).toList()))
                                          .build());
        }

        private boolean isPrincipal() {
            return params.member().equals(principal());
        }

        private Member principal() {
            return coordinator.getContext().ring(0).get(principal);
        }

    }

    private record coordinationMsg(Digest from, Coordinate coord) {}

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final BlockProducer                                blockProducer;
    private final CommonCommunications<Terminal, ?>            comms;
    private final Controller                                   controller;
    private final ReliableBroadcaster                          coordinator;
    private final Ethereal                                     ethereal;
    private final Fsm<Driven, Transitions>                     fsm;
    private final SimpleChannel<coordinationMsg>               linear;
    private final AtomicReference<Digest>                      nextViewId      = new AtomicReference<>();
    private final Parameters                                   params;
    private final Map<Digest, CertifiedBlock.Builder>          pending         = new ConcurrentHashMap<>();
    private final AtomicReference<HashedBlock>                 previousBlock   = new AtomicReference<>();
    private final Set<Digest>                                  published       = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Consumer<HashedCertifiedBlock>               publisher;
    private final CertifiedBlock.Builder                       reconfiguration = CertifiedBlock.newBuilder();
    private final BiFunction<Map<Member, Join>, Digest, Block> reconfigureBlock;
    private final Map<Digest, Short>                           roster          = new HashMap<>();
    private final RoundScheduler                               roundScheduler;
    private final Signer                                       signer;
    private final BlockingDeque<Transaction>                   transactions    = new LinkedBlockingDeque<>();
    private final Transitions                                  transitions;

    public Producer(nextView viewMember, ReliableBroadcaster coordinator, CommonCommunications<Terminal, ?> comms,
                    Parameters p, BiFunction<Map<Member, Join>, Digest, Block> reconfigureBlock,
                    Consumer<HashedCertifiedBlock> publisher, List<Digest> order, BlockProducer blockProducer,
                    HashedBlock lastBlock) {
        assert comms != null && p != null;
        this.params = p;
        this.comms = comms;
        this.reconfigureBlock = reconfigureBlock;
        this.publisher = publisher;
        this.blockProducer = blockProducer;
        this.previousBlock.set(lastBlock);
        signer = new SignerImpl(0, viewMember.consensusKeyPair().getPrivate());
        log.trace("Signing key: {} on: {}", Hex.hex(viewMember.consensusKeyPair().getPublic().getEncoded()),
                  params.member());
        short i = 0;
        for (var d : order) {
            roster.put(d, i++);
        }

        // Ethereal consensus
        ethereal = new Ethereal();

        // Reliable broadcast of both Units and Coordination messages between valid
        // members of this committee
        this.coordinator = coordinator;
        this.coordinator.registerHandler((ctx, msgs) -> msgs.forEach(msg -> process(msg)));
        roundScheduler = new RoundScheduler(coordinator.getContext().getRingCount());
        coordinator.register(tick -> roundScheduler.tick(tick));

        // FSM driving this Earner
        fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName(params.member().getId().toString());
        transitions = fsm.getTransitions();

        // buffer for coordination messages
        linear = new SimpleChannel<>("Publisher linear for: " + params.member(), 100);
        linear.consumeEach(coordination -> coordinate(coordination));

        Config.Builder config = params.ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        Short pid = roster.get(params.member().getId());
        if (pid == null) {
            config.setPid((short) 0).setnProc((short) 1);
        } else {
            log.trace("Pid: {} for: {} on: {}", pid, getViewId(), params.member());
            config.setPid(pid).setnProc((short) roster.size());
        }

        // Our handle on consensus
        controller = ethereal.deterministic(config.build(), dataSource(), (preblock, last) -> create(preblock, last),
                                            preUnit -> broadcast(preUnit));

        log.debug("Roster for: {} is: {} on: {}", getViewId(), roster, params.member());
    }

    public void complete() {
        log.debug("Closing producer for: {} on: {}", getViewId(), params.member());
        controller.stop();
        linear.close();
        coordinator.stop();
    }

    public void regenerate() {
        transitions.synchronize();
    }

    public void start() {
        transitions.start();
    }

    void setNextViewId(Digest nextViewId) {
        log.debug("Regenerating next view: {} from: {} on: {}", nextViewId, getViewId(), params.member());
        this.nextViewId.set(nextViewId);
    }

    /**
     * Reliably broadcast this preUnit to all valid members of this committee
     */
    private void broadcast(PreUnit preUnit) {
        if (metrics() != null) {
            metrics().broadcast(preUnit);
        }
        log.trace("Broadcasting: {} for: {} on: {}", preUnit, getViewId(), params.member());
        coordinator.publish(Coordinate.newBuilder().setUnit(preUnit.toPreUnit_s()).build());
    }

    /**
     * Dispatch the coordination message through the FSM
     */
    private void coordinate(coordinationMsg coord) {
        switch (coord.coord.getMsgCase()) {
        case PUBLISH:
            transitions.publish(coord.coord.getPublish());
            break;
        case RECONFIGURE:
            transitions.reconfigure(coord.coord.getReconfigure());
            break;
        case VALIDATE:
            transitions.validate(coord.coord.getValidate());
            break;
        case JOINS:
            transitions.joins(coord.coord.getJoins());
            break;
        case SYNC:
            transitions.sync(coord.coord.getSync(), coord.from);
            break;
        case VIEWVALIDATE:
            transitions.validateView(coord.coord.getValidate());
            break;
        default:
            break;
        }
    }

    /**
     * Block creation
     * 
     * @param last
     */
    private void create(PreBlock preblock, boolean last) {
        var builder = Executions.newBuilder();
        preblock.data().stream().map(e -> {
            try {
                return Executions.parseFrom(e);
            } catch (InvalidProtocolBufferException ex) {
                log.error("Error parsing transaction executions on: {}", params.member());
                return (Executions) null;
            }
        }).filter(e -> e != null).flatMap(e -> e.getExecutionsList().stream()).forEach(e -> builder.addExecutions(e));
        final HashedBlock lb = previousBlock.get();
        var next = new HashedBlock(params.digestAlgorithm(),
                                   blockProducer.produce(lb.height() + 1, lb.hash, builder.build()));
        previousBlock.set(next);
        var validation = generateValidation(next.hash, next.block);
        coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
        var cb = pending.computeIfAbsent(next.hash, h -> CertifiedBlock.newBuilder());
        cb.setBlock(next.block);
        cb.addCertifications(validation.getWitness());
        log.debug("Block: {} height: {} last: {} created on: {}", next.hash, next.height(), last, params.member());
        if (last) {
            transitions.drain();
        }
        maybePublish(next.hash, cb);
    }

    /**
     * DataSource that feeds Ethereal consensus
     */
    private DataSource dataSource() {
        return new DataSource() {
            @Override
            public ByteString getData() {
                return Producer.this.getData();
            }
        };
    }

    private Validate generateValidation(Digest hash, Block block) {
        byte[] bytes = hash(block.getHeader(), params.digestAlgorithm()).getBytes();
        log.trace("Signing block: {} height: {} on: {}", hash, height(block), params.member());
        JohnHancock signature = signer.sign(bytes);
        if (signature == null) {
            log.error("Unable to sign block: {} height: {} on: {}", hash, height(block), params.member());
            return null;
        }
        var validation = Validate.newBuilder().setHash(hash.toDigeste())
                                 .setWitness(Certification.newBuilder().setId(params.member().getId().toDigeste())
                                                          .setSignature(signature.toSig()).build())
                                 .build();
        return validation;
    }

    /**
     * The data to be used for a the next Unit produced by this Producer
     */
    private ByteString getData() {
        Executions.Builder builder = Executions.newBuilder();
        int bytesRemaining = params.maxBatchByteSize();
        int txnsRemaining = params.maxBatchSize();
        while (txnsRemaining > 0 && transactions.peek() != null
        && bytesRemaining >= transactions.peek().getSerializedSize()) {
            txnsRemaining--;
            Transaction next = transactions.poll();
            bytesRemaining -= next.getSerializedSize();
            builder.addExecutions(ExecutedTransaction.newBuilder().setTransation(next));
        }
        if (builder.getExecutionsCount() == 0) {
            ExecutedTransaction et = ExecutedTransaction.newBuilder()
                                                        .setTransation(Transaction.newBuilder()
                                                                                  .setContent(ByteString.copyFromUtf8("Give me food or give me slack or kill me")))
                                                        .build();
            builder.addExecutions(et);
            bytesRemaining -= et.getSerializedSize();
            txnsRemaining--;
        }
        int byteSize = params.maxBatchByteSize() - bytesRemaining;
        int batchSize = params.maxBatchSize() - txnsRemaining;
        if (metrics() != null) {
            metrics().publishedBatch(batchSize, byteSize);
        }
        log.trace("Produced: {} txns totalling: {} bytes pid: {} on: {}", batchSize, byteSize,
                  roster.get(params.member().getId()), params.member());
        return builder.build().toByteString();
    }

    private Digest getViewId() {
        return coordinator.getContext().getId();
    }

    private void maybePublish(Digest hash, CertifiedBlock.Builder cb) {
        final int toleranceLevel = params.context().toleranceLevel();
        if (cb.hasBlock() && cb.getCertificationsCount() > toleranceLevel) {
            var hcb = new HashedCertifiedBlock(params.digestAlgorithm(), cb.build());
            published.add(hcb.hash);
            pending.remove(hcb.hash);
            publisher.accept(hcb);
            log.debug("Block: {} height: {} certs: {} > {} published on: {}", hcb.hash, hcb.height(),
                      hcb.certifiedBlock.getCertificationsCount(), toleranceLevel, params.member());
            transitions.publishedBlock();
        } else if (cb.hasBlock()) {
            log.trace("Block: {} height: {} pending: {} <= {} on: {}", hash, height(cb.getBlock()),
                      cb.getCertificationsCount(), toleranceLevel, params.member());
        } else {
            log.trace("Block: {} empty, pending: {} on: {}", hash, cb.getCertificationsCount(), params.member());
        }
    }

    private ChoamMetrics metrics() {
        return params.metrics();
    }

    /**
     * Reliable broadcast message processing
     */
    private void process(Msg msg) {
        Coordinate coordination;
        try {
            coordination = Coordinate.parseFrom(msg.content());
        } catch (InvalidProtocolBufferException e) {
            log.debug("Error deserializing from: {} on: {}", msg.source(), params.member());
            if (metrics() != null) {
                metrics().coordDeserEx();
            }
            return;
        }
        log.trace("Received msg from: {} type: {} on: {}", msg.source(), coordination.getMsgCase(), params.member());
        if (metrics() != null) {
            metrics().incTotalMessages();
        }
        if (coordination.hasUnit()) {
            Short source = roster.get(msg.source());
            if (source == null) {
                log.debug("No pid in roster: {} matching: {} on: {}", roster, msg.source(), params.member());
                if (metrics() != null) {
                    metrics().invalidSourcePid();
                }
                return;
            }
            publish(msg.source(), source, PreUnit.from(coordination.getUnit(), params.digestAlgorithm()));
        } else {
            linear.submit(new coordinationMsg(msg.source(), coordination));
        }
    }

    /**
     * Publish or perish
     */
    private void publish(Digest member, short source, preUnit pu) {
        if (pu.creator() != source) {
            log.debug("Received invalid unit: {} from: {} should be creator: {} on: {}", pu, member, source,
                      params.member());
            if (metrics() != null) {
                metrics().invalidUnit();
            }
            return;
        }
        log.trace("Received unit: {} source pid: {} member: {} on: {}", pu, source, member, params.member());
        controller.input().accept(source, Collections.singletonList(pu));
    }

}
