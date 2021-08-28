/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.support.HashedBlock.height;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Assemble;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesfoce.apollo.choam.proto.Endorsement;
import com.salesfoce.apollo.choam.proto.Endorsements;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Outcome;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.choam.fsm.Driven;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;
import com.salesforce.apollo.choam.fsm.Earner;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.TxDataSource;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Config.Builder;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.utils.SimpleChannel;

/**
 * An "Earner"
 * 
 * @author hal.hildebrand
 *
 */
public class Producer {

    /** Leaf action driver coupling for the Producer FSM */
    private class DriveIn implements Driven {

        @Override
        public void prepareAssembly() {
            controller.stop();
            final Digest next = nextViewId;
            log.debug("Preparing Assembly of next view: {} from: {} on: {}", next, getViewId(), params().member());
            AtomicInteger countDown = new AtomicInteger(3);
            AtomicReference<UUID> registration = new AtomicReference<>();
            registration.set(coordinator.register(round -> {
                if (round % coordinator.getContext().timeToLive() == 0) {
                    if (!pending.isEmpty() && countDown.decrementAndGet() > 0) {
                        pending.values().forEach(b -> {
                            var validation = view.generateValidation(new HashedBlock(params().digestAlgorithm(),
                                                                                     b.getBlock()));
                            coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
                        });
                    } else {
                        coordinator.removeRoundListener(registration.get());
                        transitions.lastBlock();
                    }
                }
            }));
        }

        @Override
        public void reconfigure() {
            var assembly = joins.entrySet().stream()
                                .collect(Collectors.toMap(e -> e.getKey(), e -> cannonical(e.getValue()))).entrySet()
                                .stream()
                                .filter(e -> e.getValue().getEndorsementsCount() > params().context().toleranceLevel())
                                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
            if (assembly.size() <= params().context().toleranceLevel()) {
                log.error("Next view: {} from: {} joins: {} required: {} regeneration failed on: {}", nextViewId,
                          getViewId(), assembly.size(), params().context().toleranceLevel() + 1, params().member());
                transitions.assemblyFailed();
                return;
            }

            final HashedBlock lb = previousBlock.get();
            var reconfigure = new HashedBlock(params().digestAlgorithm(),
                                              blockProducer.reconfigure(assembly, lb.hash, lb));
            log.debug("Consensus complete, next view: {} from: {} on: {}", lb.hash, getViewId(), params().member());

            previousBlock.set(reconfigure);
            var validation = view.generateValidation(reconfigure);
            coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
            var cb = pending.computeIfAbsent(reconfigure.hash, h -> CertifiedBlock.newBuilder());
            cb.setBlock(reconfigure.block);
            cb.addCertifications(validation.getWitness());
            log.debug("Reconfiguration block: {} height: {} last: {} created on: {}", reconfigure.hash,
                      reconfigure.height(), lb, params().member());
            maybePublish(reconfigure.hash, cb);
        }

        @Override
        public void startProduction() {
            log.debug("Starting production of: {} on: {}", getViewId(), params().member());
            coordinator.start(params().gossipDuration(), params().scheduler());
            final Controller current = controller;
            current.start();
        }

        @Override
        public void submit(Transaction transaction, CompletableFuture<SubmitResult> result) {
            if (ds.offer(transaction)) {
                log.debug("Submitted received txn: {} on: {}", CHOAM.hashOf(transaction, params().digestAlgorithm()),
                          params().member());
                result.complete(SubmitResult.newBuilder().setOutcome(Outcome.SUCCESS).build());
            } else {
                log.warn("Failure, cannot submit received txn: {} on: {}",
                         CHOAM.hashOf(transaction, params().digestAlgorithm()), params().member());
                result.complete(SubmitResult.newBuilder().setOutcome(Outcome.FAILURE).build());
            }
        }

        @Override
        public void valdateBlock(Validate validate) {
            var hash = new Digest(validate.getHash());
            if (published.contains(hash)) {
                log.debug("Block: {} already published on: {}", hash, params().member());
                return;
            }
            var p = pending.computeIfAbsent(hash, h -> CertifiedBlock.newBuilder());
            p.addCertifications(validate.getWitness());
            log.trace("Validation for block: {} height: {} on: {}", hash,
                      p.hasBlock() ? height(p.getBlock()) : "missing", params().member());
            maybePublish(hash, p);
        }

        @SuppressWarnings("unused")
        private boolean validate(Validate validate, CertifiedBlock.Builder p) {
            Digest id = new Digest(validate.getWitness().getId());
            Member witness = coordinator.getContext().getMember(id);
            if (witness == null) {
                log.trace("Invalid witness: {} on: {}", id, id, params().member());
            }
            return false;
        }

        @Override
        public void preSpice() {
            log.debug("Pre Spice phase started for: {} from: {} on: {}", nextViewId, getViewId(), params().member());
            coordinator.stop();
            Digest preSpiceId = view.context().getId().prefix("-PreSpice".getBytes());
            final Context<Member> preSpiceContext = new Context<>(preSpiceId, view.context().getRingCount());
            view.context().allMembers().forEach(e -> preSpiceContext.activate(e));
            coordinator = new ReliableBroadcaster(params().coordination().clone().setMember(params().member())
                                                          .setContext(preSpiceContext).build(),
                                                  params().communications());
            coordinator.registerHandler((ctx, msgs) -> msgs.forEach(msg -> process(msg)));
            coordinator.start(params().gossipDuration(), params().scheduler());
            initializeConsensus();
            controller.start();
            produceAssemble();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static Join cannonical(Join.Builder proto) {
        List<Certification> endorsements = new ArrayList<>(proto.getEndorsementsList());
        proto.clearEndorsements();
        endorsements.sort(Comparator.comparing(c -> new Digest(c.getId())));
        proto.addAllEndorsements(endorsements);
        return proto.build();
    }

    private final BlockProducer                       blockProducer;
    private volatile Controller                       controller;
    private volatile ReliableBroadcaster              coordinator;
    private final TxDataSource                        ds;
    private final Map<Member, Join.Builder>           joins         = new ConcurrentHashMap<>();
    private final SimpleChannel<Coordinate>           linear;
    private volatile Digest                           nextViewId;
    private final Map<Digest, CertifiedBlock.Builder> pending       = new ConcurrentHashMap<>();
    private final AtomicReference<HashedBlock>        previousBlock = new AtomicReference<>();
    private final Set<Digest>                         published     = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Transitions                         transitions;
    private final ViewContext                         view;

    public Producer(ViewContext view, HashedBlock lastBlock, BlockProducer blockProducer) {
        assert view != null;
        this.view = view;
        this.previousBlock.set(lastBlock);
        this.blockProducer = blockProducer;

        final Parameters params = view.params();
        final Builder ep = params.ethereal();
        ds = new TxDataSource(params,
                              (params.maxBatchByteSize() * ((ep.getEpochLength() * ep.getNumberOfEpochs()) - 3)) * 2);

        // Reliable broadcast of both Unit and Coordination messages between valid
        // members of this committee
        coordinator = new ReliableBroadcaster(params.coordination().clone().setMember(params.member())
                                                    .setContext(view.context()).build(),
                                              params.communications());
        coordinator.registerHandler((ctx, msgs) -> msgs.forEach(msg -> process(msg)));

        var fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName(params().member().getId().toString());
        transitions = fsm.getTransitions();

        // buffer for coordination messages
        linear = new SimpleChannel<>("Publisher linear for: " + params().member(), 100);
        linear.consumeEach(coordination -> transitions.validate(coordination.getValidate()));

        initializeConsensus();
    }

    public void complete() {
        log.debug("Closing producer for: {} on: {}", getViewId(), params().member());
        final Controller current = controller;
        current.stop();
        linear.close();
        coordinator.stop();
    }

    public void endorsement(Endorsements endorsements) {
        final Digest next = nextViewId;
        if (next == null) {
            log.debug("No next view from: {} on: {}", getViewId(), params().member());
            return;
        }
        Digest id = new Digest(endorsements.getMember());
        Member member = view.context().getMember(id);
        var assembly = Committee.viewMembersOf(next, params().context());
        if (member == null) {
            log.debug("Invalid endorsement for view: {} from non member: {} on: {}", getViewId(), id,
                      params().member());
            return;
        }
        endorsements.getEndorsementsList().stream().forEach(e -> record(e, assembly, member));
    }

    public Digest getNextViewId() {
        final Digest current = nextViewId;
        return current;
    }

    public void join(Join join) {
        Digest view = new Digest(join.getView());
        Digest memberId = new Digest(join.getMember().getId());
        final Digest next = nextViewId;
        if (next == null) {
            log.debug("No view for join: {} current: {} from: {} on: {}", view, getViewId(), memberId,
                      params().member());
            return;
        }
        if (!next.equals(view)) {
            log.debug("Join view: {} does not match current: {} from: {} on: {}", view, getViewId(), memberId,
                      params().member());
            return;
        }
        Member member = params().context().getMember(memberId);
        if (member == null) {
            log.debug("Invalid member join: {} current: {} from: {} on: {}", view, getViewId(), memberId,
                      params().member());
            return;
        }
        if (!Committee.viewMembersOf(next, params().context()).contains(member)) {
            log.debug("Member not a committee member of: {} current: {} from: {} on: {}", view, getViewId(), memberId,
                      params().member());
            return;
        }
        JohnHancock sig = JohnHancock.of(join.getMember().getSignature());
        if (!member.verify(sig, join.getMember().getConsensusKey().toByteString())) {
            log.debug("Cannot validate consensus key for: {} current: {} from: {} on: {}", view, getViewId(), memberId,
                      params().member());
            return;
        }
        log.debug("Adding join for: {} current: {} from: {} on: {}", view, getViewId(), memberId, params().member());
        // looks good to me... First txn wins rule
        joins.putIfAbsent(member, Join.newBuilder(join));
    }

    public void start() {
        log.debug("Starting production for: {} on: {}", getViewId(), params().member());
        transitions.start();
    }

    public SubmitResult submit(Transaction transaction) {
        log.trace("Submit received txn: {} on: {}", CHOAM.hashOf(transaction, params().digestAlgorithm()),
                  params().member());
        CompletableFuture<SubmitResult> result = new CompletableFuture<SubmitResult>();
        transitions.submit(transaction, result);
        try {
            return result.get();
        } catch (InterruptedException e) {
            log.warn("Failure to submit received txn: {} on: {}", CHOAM.hashOf(transaction, params().digestAlgorithm()),
                     params().member(), e);
            return SubmitResult.newBuilder().setOutcome(Outcome.FAILURE).build();
        } catch (ExecutionException e) {
            log.debug("Failure to submit received txn:{} on: {}", CHOAM.hashOf(transaction, params().digestAlgorithm()),
                      params().member(), e.getCause());
            return SubmitResult.newBuilder().setOutcome(Outcome.FAILURE).build();
        }
    }

    /**
     * Reliably broadcast this preUnit to all valid members of this committee
     */
    private void broadcast(PreUnit preUnit) {
        if (metrics() != null) {
            metrics().broadcast(preUnit);
        }
        log.trace("Broadcasting: {} for: {} on: {}", preUnit, getViewId(), params().member());
        coordinator.publish(Coordinate.newBuilder().setUnit(preUnit.toPreUnit_s()).build());
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
                log.error("Error parsing transaction executions on: {}", params().member());
                return (Executions) null;
            }
        }).filter(e -> e != null).flatMap(e -> e.getExecutionsList().stream()).forEach(e -> builder.addExecutions(e));
        HashedBlock lb = previousBlock.get();

        var next = new HashedBlock(params().digestAlgorithm(), view.produce(lb.height() + 1, lb.hash, builder.build()));
        previousBlock.set(next);
        var validation = view.generateValidation(next);
        coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
        var cb = pending.computeIfAbsent(next.hash, h -> CertifiedBlock.newBuilder());
        cb.setBlock(next.block);
        cb.addCertifications(validation.getWitness());
        maybePublish(next.hash, cb);
        log.debug("Block: {} height: {} created on: {}", next.hash, next.height(), params().member());
        if (last) {
            transitions.lastBlock();
        }
    }

    private Digest getViewId() {
        return coordinator.getContext().getId();
    }

    private void initializeConsensus() {
        Config.Builder config = params().ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        Short pid = view.roster().get(params().member().getId());
        if (pid == null) {
            config.setPid((short) 0).setnProc((short) 1);
        } else {
            log.trace("Pid: {} for: {} on: {}", pid, getViewId(), params().member());
            config.setPid(pid).setnProc((short) view.roster().size());
        }

        // Ethereal consensus
        var ethereal = new Ethereal();
        // Our handle on consensus
        controller = ethereal.deterministic(config.build(), ds, (preblock, last) -> create(preblock, last),
                                            preUnit -> broadcast(preUnit));
        assert controller != null : "Controller is null";

        log.debug("Roster for: {} is: {} on: {}", getViewId(), view.roster(), params().member());
    }

    private void maybePublish(Digest hash, CertifiedBlock.Builder cb) {
        final int toleranceLevel = params().context().toleranceLevel();
        if (cb.hasBlock() && cb.getCertificationsCount() > toleranceLevel) {
            var hcb = new HashedCertifiedBlock(params().digestAlgorithm(), cb.build());
            published.add(hcb.hash);
            pending.remove(hcb.hash);
            view.publish(hcb);
            log.debug("Block: {} height: {} certs: {} > {} published on: {}", hcb.hash, hcb.height(),
                      hcb.certifiedBlock.getCertificationsCount(), toleranceLevel, params().member());
        } else if (cb.hasBlock()) {
            log.trace("Block: {} height: {} pending: {} <= {} on: {}", hash, height(cb.getBlock()),
                      cb.getCertificationsCount(), toleranceLevel, params().member());
        } else {
            log.trace("Block: {} empty, pending: {} on: {}", hash, cb.getCertificationsCount(), params().member());
        }
    }

    private ChoamMetrics metrics() {
        return params().metrics();
    }

    private Parameters params() {
        return view.params();
    }

    /**
     * Reliable broadcast message processing
     */
    private void process(Msg msg) {
        Coordinate coordination;
        try {
            coordination = Coordinate.parseFrom(msg.content());
        } catch (InvalidProtocolBufferException e) {
            log.debug("Error deserializing from: {} on: {}", msg.source(), params().member());
            if (metrics() != null) {
                metrics().coordDeserialError();
            }
            return;
        }
        log.trace("Received msg from: {} type: {} on: {}", msg.source(), coordination.getMsgCase(), params().member());
        if (metrics() != null) {
            metrics().incTotalMessages();
        }
        if (coordination.hasUnit()) {
            Short source = view.roster().get(msg.source());
            if (source == null) {
                log.debug("No pid in roster: {} matching: {} on: {}", view.roster(), msg.source(), params().member());
                if (metrics() != null) {
                    metrics().invalidSourcePid();
                }
                return;
            }
            publish(msg.source(), source, PreUnit.from(coordination.getUnit(), params().digestAlgorithm()));
        } else {
            linear.submit(coordination);
        }
    }

    private void produceAssemble() {
        final var vlb = previousBlock.get();
        nextViewId = vlb.hash;
        final var reconfigure = new HashedBlock(params().digestAlgorithm(), view.produce(vlb.height()
        + 1, vlb.hash, Assemble.newBuilder().setNextView(vlb.hash.toDigeste()).build()));
        previousBlock.set(reconfigure);
        final var validation = view.generateValidation(reconfigure);
        coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
        final var rcb = pending.computeIfAbsent(reconfigure.hash, h -> CertifiedBlock.newBuilder());
        rcb.setBlock(reconfigure.block);
        rcb.addCertifications(validation.getWitness());
        log.debug("Next view created: {} height: {} body: {} from: {} on: {}", reconfigure.hash, reconfigure.height(),
                  reconfigure.block.getBodyCase(), getViewId(), params().member());
        maybePublish(reconfigure.hash, rcb);
        AtomicReference<UUID> registration = new AtomicReference<>();
        registration.set(coordinator.register(round -> {
            if (round % coordinator.getContext().timeToLive() == 0) {
                if (!pending.containsKey(reconfigure.hash)) {
                    pending.values().forEach(b -> {
                        coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
                    });
                } else {
                    coordinator.removeRoundListener(registration.get());
                }
            }
        }));
    }

    /**
     * Publish or perish
     */
    private void publish(Digest member, short source, preUnit pu) {
        if (pu.creator() != source) {
            log.debug("Received invalid unit: {} from: {} should be creator: {} on: {}", pu, member, source,
                      params().member());
            if (metrics() != null) {
                metrics().invalidUnit();
            }
            return;
        }
        log.trace("Received unit: {} source pid: {} member: {} on: {}", pu, source, member, params().member());
        final Controller current = controller;
        current.input().accept(source, Collections.singletonList(pu));
    }

    private void record(Endorsement e, Set<Member> assembly, Member witness) {
        Digest id = new Digest(e.getViewMember());
        Member member = params().context().getMember(id);
        if (member == null) {
            log.debug("Invalid endorsement for view: {} for non member: {} on: {}", getViewId(), id, params().member());
            return;
        }
        if (!assembly.contains(member)) {
            log.debug("Invalid endorsement for view: {} for undelegated member: {} on: {}", getViewId(), id,
                      params().member());
            return;
        }
        Join.Builder b = joins.get(member);
        if (b == null) {
            log.debug("Not Join registered for endorsement for view: {} for: {} on: {}", getViewId(), id,
                      params().member());
            return;
        }

        if (!witness.verify(JohnHancock.of(e.getSignature()), b.getMember().getConsensusKey().toByteString())) {
            log.debug("Cannot validate endorsement for view: {} for: {} on: {}", getViewId(), id, params().member());
            return;
        }
        // Looks good to me...
        b.addEndorsements(Certification.newBuilder().setId(member.getId().toDigeste()).setSignature(e.getSignature()));
    }
}
