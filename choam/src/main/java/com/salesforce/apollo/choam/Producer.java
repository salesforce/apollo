/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.fsm.Driven.PERIODIC_VALIDATIONS;
import static com.salesforce.apollo.choam.fsm.Driven.SYNC;
import static com.salesforce.apollo.choam.support.HashedBlock.height;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Assemble;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Outcome;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.ethereal.proto.ChRbcMessage;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Driven;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;
import com.salesforce.apollo.choam.fsm.Earner;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.TxDataSource;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
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
import com.salesforce.apollo.utils.RoundScheduler;

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
        public void cancelTimers() {
            scheduler.cancelAll();
        }

        @Override
        public void checkpoint() {
            Block ckpt = view.checkpoint();
            if (ckpt == null) {
                log.error("Cannot generate checkpoint block on: {}", params().member());
                transitions.failed();
                return;
            }
            coordinator.start(params().producer().gossipDuration(), params().scheduler());
            var next = new HashedBlock(params().digestAlgorithm(), ckpt);
            previousBlock.set(next);
            var validation = view.generateValidation(next);
            coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
            var cb = pending.computeIfAbsent(next.hash, h -> CertifiedBlock.newBuilder());
            cb.setBlock(next.block);
            cb.addCertifications(validation.getWitness());
            maybePublish(next.hash, cb);
            log.info("Produced checkpoint: {} height: {} for: {} on: {}", next.hash, next.height(), getViewId(),
                     params().member());
            transitions.lastBlock();
        }

        @Override
        public void complete() {
            Producer.this.complete();
        }

        @Override
        public void reconfigure() {
            log.debug("Attempting assembly of: {} assembled: {} on: {}", nextViewId, joins.size(), params().member());

            int toleranceLevel = params().toleranceLevel();
            log.debug("Aggregate of: {} joins: {} on: {}", nextViewId, joins.size(), params().member());
            if (joins.size() > toleranceLevel) {
                var reconfigure = view.reconfigure(joins, nextViewId, previousBlock.get());
                var rhb = new HashedBlock(params().digestAlgorithm(), reconfigure);
                var validation = view.generateValidation(rhb);
                coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
                log.debug("Aggregate of: {} threshold reached: {} block: {} on: {}", nextViewId, joins.size(), rhb.hash,
                          params().member());
                var cb = pending.computeIfAbsent(rhb.hash, h -> CertifiedBlock.newBuilder());
                cb.setBlock(rhb.block);
                cb.addCertifications(validation.getWitness());
                maybePublish(rhb.hash, cb);
                log.debug("Reconfiguration block: {} height: {} created on: {}", rhb.hash, rhb.height(),
                          params().member());
            } else {
                log.warn("Aggregate of: {} threshold failed: {} required: {} on: {}", nextViewId, joins.size(),
                         toleranceLevel + 1, params().member());
                transitions.failed();
            }
            periodicValidations(() -> transitions.lastBlock());
        }

        @Override
        public void startProduction() {
            log.info("Starting production for: {} on: {}", getViewId(), params().member());
            coordinator.start(params().producer().gossipDuration(), params().scheduler());
            final Controller current = controller;
            current.start();
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
    }

    private static final Logger                       log           = LoggerFactory.getLogger(Producer.class);
    private volatile ViewAssembly                     assembly;
    private final AtomicBoolean                       closed        = new AtomicBoolean(false);
    private final CommonCommunications<Terminal, ?>   comms;
    private final Controller                          controller;
    private final ReliableBroadcaster                 coordinator;
    private final TxDataSource                        ds;
    private final Map<Member, Join>                   joins         = new ConcurrentHashMap<>();
    private final ExecutorService                     linear;
    private final Set<Member>                         nextAssembly  = new HashSet<>();
    private volatile Digest                           nextViewId;
    private final Map<Digest, CertifiedBlock.Builder> pending       = new ConcurrentHashMap<>();
    private final AtomicReference<HashedBlock>        previousBlock = new AtomicReference<>();
    private final Set<Digest>                         published     = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final AtomicInteger                       reconfigureCountdown;
    private final RoundScheduler                      scheduler;
    private final Transitions                         transitions;
    private final ViewContext                         view;

    public Producer(ViewContext view, HashedBlock lastBlock, CommonCommunications<Terminal, ?> comms) {
        assert view != null;
        this.view = view;
        this.previousBlock.set(lastBlock);
        this.comms = comms;
        this.reconfigureCountdown = new AtomicInteger(2); // TODO params

        final Parameters params = view.params();
        final Builder ep = params.producer().ethereal();
        final int maxBufferSize = (params.producer().maxBatchByteSize()
        * ((ep.getEpochLength() * ep.getNumberOfEpochs()) - 3));
        ds = new TxDataSource(params, maxBufferSize);
        final Context<Member> context = view.context();

        coordinator = new ReliableBroadcaster(params().producer().coordination().clone().setMember(params().member())
                                                      .setContext(context).build(),
                                              params().communications());
        coordinator.registerHandler((ctx, msgs) -> msgs.forEach(msg -> process(msg)));
        scheduler = new RoundScheduler(context.getRingCount());
        coordinator.register(i -> scheduler.tick(i));

        var fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName(params().member().getId().toString());
        transitions = fsm.getTransitions();

        // buffer for coordination messages
        linear = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "Linear Producer " + params.member().getId());
            thread.setDaemon(true);
            return thread;
        });

        Config.Builder config = params().producer().ethereal().clone();

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
                                            preUnit -> broadcast(preUnit), scheduler);
        assert controller != null : "Controller is null";

        log.debug("Roster for: {} is: {} on: {}", getViewId(), view.roster(), params().member());
    }

    public void complete() {
        log.info("Closing producer for: {} on: {}", getViewId(), params().member());
        controller.stop();
        linear.shutdown();
        coordinator.stop();
        if (assembly != null) {
            assembly.complete();
        }
    }

    public Digest getNextViewId() {
        final Digest current = nextViewId;
        return current;
    }

    public void start() {
        final Block prev = previousBlock.get().block;
        if (prev.hasReconfigure() && prev.getReconfigure().getCheckpointTarget() == 0) { // genesis block won't ever be
                                                                                         // 0
            transitions.checkpoint();
        } else {
            transitions.start();
        }
        periodicValidations(null);
    }

    public SubmitResult submit(Transaction transaction) {
        log.trace("Submit received txn: {} on: {}", CHOAM.hashOf(transaction, params().digestAlgorithm()),
                  params().member());
        if (closed.get()) {
            log.trace("Failure, cannot submit received txn: {} on: {}",
                      CHOAM.hashOf(transaction, params().digestAlgorithm()), params().member());
            return SubmitResult.newBuilder().setOutcome(Outcome.INACTIVE_COMMITTEE).build();
        }

        if (ds.offer(transaction)) {
            log.debug("Submitted received txn: {} on: {}", CHOAM.hashOf(transaction, params().digestAlgorithm()),
                      params().member());
            return SubmitResult.newBuilder().setOutcome(Outcome.SUCCESS).build();
        } else {
            log.debug("Failure, cannot submit received txn: {} on: {}",
                      CHOAM.hashOf(transaction, params().digestAlgorithm()), params().member());
            return SubmitResult.newBuilder().setOutcome(Outcome.FAILURE).build();
        }
    }

    /**
     * Reliably broadcast this preUnit to all valid members of this committee
     */
    private void broadcast(ChRbcMessage msg) {
        var preUnit = PreUnit.from(msg.getPropose(), params().digestAlgorithm());
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
        scheduler.cancel(SYNC);
        var builder = Executions.newBuilder();
        var aggregate = preblock.data().stream().map(e -> {
            try {
                return Executions.parseFrom(e);
            } catch (InvalidProtocolBufferException ex) {
                log.error("Error parsing transaction executions on: {}", params().member());
                return (Executions) null;
            }
        }).filter(e -> e != null).toList();

        aggregate.stream().flatMap(e -> e.getExecutionsList().stream()).forEach(e -> builder.addExecutions(e));
        aggregate.stream().flatMap(e -> e.getJoinsList().stream()).forEach(j -> builder.addJoins(j));

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
            closed.set(true);
            transitions.lastBlock();
        } else if (reconfigureCountdown.decrementAndGet() == 0) {
            produceAssemble();
        }
    }

    private Digest getViewId() {
        return coordinator.getContext().getId();
    }

    private void maybePublish(Digest hash, CertifiedBlock.Builder cb) {
        final int toleranceLevel = params().toleranceLevel();
        if (cb.hasBlock() && cb.getCertificationsCount() > toleranceLevel) {
            var hcb = new HashedCertifiedBlock(params().digestAlgorithm(), cb.build());
            published.add(hcb.hash);
            pending.remove(hcb.hash);
            view.publish(hcb);
            log.trace("Block: {} height: {} certs: {} > {} published on: {}", hcb.hash, hcb.height(),
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

    private void periodicValidations(Runnable onEmptyAction) {
        AtomicReference<Runnable> action = new AtomicReference<>();
        action.set(() -> {
            if (pending.isEmpty() && onEmptyAction != null) {
                onEmptyAction.run();
                return;
            }
            pending.values().forEach(b -> {
                final HashedBlock hb = new HashedBlock(params().digestAlgorithm(), b.getBlock());
                var validation = view.generateValidation(hb);
                coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
                log.trace("Published periodic validation of: {} height: {} on: {}", hb.hash, hb.height(),
                          params().member());
            });
            scheduler.schedule(PERIODIC_VALIDATIONS, action.get(), 1);
        });
        log.trace("Scheduling periodic validations on: {}", params().member());
        scheduler.schedule(PERIODIC_VALIDATIONS, action.get(), 1);
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
            linear.execute(() -> valdateBlock(coordination.getValidate()));
        }
    }

    private void produceAssemble() {
        final var vlb = previousBlock.get();
        nextViewId = vlb.hash;
        nextAssembly.addAll(Committee.viewMembersOf(nextViewId, params().context()));
        final var reconfigure = new HashedBlock(params().digestAlgorithm(), view.produce(vlb.height()
        + 1, vlb.hash, Assemble.newBuilder().setNextView(vlb.hash.toDigeste()).build()));
        previousBlock.set(reconfigure);
        final var validation = view.generateValidation(reconfigure);
        coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
        final var rcb = pending.computeIfAbsent(reconfigure.hash, h -> CertifiedBlock.newBuilder());
        rcb.setBlock(reconfigure.block);
        rcb.addCertifications(validation.getWitness());
        log.debug("Next view: {} created: {} height: {} body: {} from: {} on: {}", nextViewId, reconfigure.hash,
                  reconfigure.height(), reconfigure.block.getBodyCase(), getViewId(), params().member());
        maybePublish(reconfigure.hash, rcb);
        assembly = new ViewAssembly(nextViewId, view, comms) {
            @Override
            protected void assembled(Map<Member, Join> aggregate) {
                joins.putAll(aggregate);
            }
        };
        assembly.start();
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

    private void valdateBlock(Validate validate) {
        var hash = new Digest(validate.getHash());
        if (published.contains(hash)) {
            log.trace("Block: {} already published on: {}", hash, params().member());
            return;
        }
        var p = pending.computeIfAbsent(hash, h -> CertifiedBlock.newBuilder());
        p.addCertifications(validate.getWitness());
        log.trace("Validation for block: {} height: {} on: {}", hash, p.hasBlock() ? height(p.getBlock()) : "missing",
                  params().member());
        maybePublish(hash, p);
    }
}
