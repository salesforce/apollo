/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Assemble;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.UnitData;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Driven;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;
import com.salesforce.apollo.choam.fsm.Earner;
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
import com.salesforce.apollo.ethereal.memberships.ContextGossiper;
import com.salesforce.apollo.membership.Member;

/**
 * An "Earner"
 * 
 * @author hal.hildebrand
 *
 */
public class Producer {

    record PendingBlock(HashedBlock block, Map<Member, Validate> witnesses, AtomicBoolean published) {}

    /** Leaf action driver coupling for the Producer FSM */
    private class DriveIn implements Driven {

        @Override
        public void assembled() {
            ds.validationsOnly();
            final var slate = assembly.getSlate();
            var reconfiguration = new HashedBlock(params().digestAlgorithm(),
                                                  view.reconfigure(slate, nextViewId, previousBlock.get()));
            var validation = view.generateValidation(reconfiguration);
            final var p = new PendingBlock(reconfiguration, new HashMap<>(), new AtomicBoolean());
            pending.put(reconfiguration.hash, p);
            p.witnesses.put(params().member(), validation);
            ds.offer(validation);
            log.info("Reconfiguration block: {} height: {} produced on: {}", reconfiguration.hash,
                     reconfiguration.height(), params().member());
        }

        @Override
        public void checkAssembly() {
            if (assembled.get()) {
                assembled();
            }
        }

        @Override
        public void checkpoint() {
            log.info("Generating checkpoint block on: {}", params().member());
            Block ckpt = view.checkpoint();
            if (ckpt == null) {
                log.error("Cannot generate checkpoint block on: {}", params().member());
                transitions.failed();
                return;
            }
            var next = new HashedBlock(params().digestAlgorithm(), ckpt);
            previousBlock.set(next);
            var validation = view.generateValidation(next);
            ds.offer(validation);
            final var p = new PendingBlock(next, new HashMap<>(), new AtomicBoolean());
            pending.put(next.hash, p);
            p.witnesses.put(params().member(), validation);
            log.info("Produced checkpoint: {} height: {} for: {} on: {}", next.hash, next.height(), getViewId(),
                     params().member());
            transitions.lastBlock();
        }

        @Override
        public void complete() {
            stop();
        }

        @Override
        public void drain() {
            draining.set(true);
            ds.drain();
            log.debug("Draining with: {} remaining batches on: {}", ds.getRemaining(), params().member());
        }

        @Override
        public void fail() {
            stop();
        }

        @Override
        public void reconfigure() {
            log.debug("Starting view reconfiguration for: {} on: {}", nextViewId, params().member());
            assembly = new ViewAssembly(nextViewId, view, comms) {
                @Override
                public void complete() {
                    super.complete();
                    log.debug("View reconfiguration: {} gathered: {} complete on: {}", nextViewId, getSlate().size(),
                              params().member());
                    assembled.set(true);
                    Producer.this.transitions.viewComplete();
                }
            };
            assembly.start();
            assembly.assembled();
        }

        @Override
        public void startProduction() {
            log.debug("Starting production for: {} on: {}", getViewId(), params().member());
            controller.start();
            coordinator.start(params().producer().gossipDuration(), params().scheduler());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final AtomicBoolean                     assembled     = new AtomicBoolean();
    private volatile ViewAssembly                   assembly;
    private final CommonCommunications<Terminal, ?> comms;
    private final Controller                        controller;
    private final ContextGossiper                   coordinator;
    private final AtomicBoolean                     draining      = new AtomicBoolean();
    private final TxDataSource                      ds;
    private final int                               lastEpoch;
    private final Set<Member>                       nextAssembly  = new HashSet<>();
    private volatile Digest                         nextViewId;
    private final Map<Digest, PendingBlock>         pending       = new ConcurrentHashMap<>();
    private final AtomicReference<HashedBlock>      previousBlock = new AtomicReference<>();
    private final AtomicBoolean                     started       = new AtomicBoolean(false);
    private final Transitions                       transitions;
    private final ViewContext                       view;

    public Producer(ViewContext view, HashedBlock lastBlock, CommonCommunications<Terminal, ?> comms) {
        assert view != null;
        this.view = view;
        this.previousBlock.set(lastBlock);
        this.comms = comms;

        final Parameters params = view.params();
        final var producerParams = params.producer();
        final Builder ep = producerParams.ethereal();

        lastEpoch = ep.getNumberOfEpochs() - 1;

        // Number of rounds we can provide data for
        final var blocks = ep.getEpochLength();
        final int maxElements = blocks * ep.getNumberOfEpochs();

        ds = new TxDataSource(params.member(), maxElements, params.metrics(), producerParams.maxBatchByteSize(),
                              producerParams.batchInterval(), producerParams.maxBatchCount());

        log.trace("Producer max elements: {} reconfiguration epoch: {} on: {}", maxElements, lastEpoch,
                  params.member());

        var fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName(params().member().getId().toString());
        transitions = fsm.getTransitions();

        Config.Builder config = params().producer().ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        Short pid = view.roster().get(params().member().getId());
        if (pid == null) {
            config.setPid((short) 0).setnProc((short) 1);
        } else {
            log.trace("Pid: {} for: {} on: {}", pid, getViewId(), params().member());
            config.setPid(pid).setnProc((short) view.roster().size());
        }

        controller = new Ethereal().deterministic(config.build(), ds, (preblock, last) -> create(preblock, last),
                                                  epoch -> newEpoch(epoch));
        coordinator = new ContextGossiper(controller, view.context(), params().member(), params().communications(),
                                          params().exec(), params().metrics());
        log.debug("Roster for: {} is: {} on: {}", getViewId(), view.roster(), params().member());
    }

    public void assembled() {
        transitions.assembled();
    }

    public Digest getNextViewId() {
        final Digest current = nextViewId;
        return current;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        final Block prev = previousBlock.get().block;
        ds.start(params().producer().batchInterval(), params().scheduler());
        if (prev.hasReconfigure() && prev.getReconfigure().getCheckpointTarget() == 0) { // genesis block won't ever be
                                                                                         // 0
            transitions.checkpoint();
        } else {
            transitions.start();
        }
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.trace("Closing producer for: {} on: {}", getViewId(), params().member());
        controller.stop();
        coordinator.stop();
        final var c = assembly;
        if (c != null) {
            c.stop();
        }
        ds.close();
    }

    public SubmitResult submit(Transaction transaction) {
        if (ds.offer(transaction)) {
            return SubmitResult.newBuilder().setSuccess(true).setStatus("OK").build();
        } else {
            return SubmitResult.newBuilder()
                               .setSuccess(false)
                               .setStatus("Transaction buffer full on: " + params().member().getId())
                               .build();
        }
    }

    private void create(PreBlock preblock, boolean last) {
        var aggregate = preblock.data().stream().map(e -> {
            try {
                return UnitData.parseFrom(e);
            } catch (InvalidProtocolBufferException ex) {
                log.error("Error parsing unit data on: {}", params().member());
                return (UnitData) null;
            }
        }).filter(e -> e != null).toList();

        aggregate.stream()
                 .flatMap(e -> e.getValidationsList().stream())
                 .map(witness -> validate(witness))
                 .filter(p -> p != null)
                 .filter(p -> !p.published.get())
                 .filter(p -> p.witnesses.size() > params().toleranceLevel())
                 .forEach(p -> publish(p));

        HashedBlock lb = previousBlock.get();
        final var txns = aggregate.stream().flatMap(e -> e.getTransactionsList().stream()).toList();

        if (draining.get() && txns.isEmpty()) {
            if (last) {
                log.debug("Draining, no txns. prev: {} height: {} last: {} on: {}", lb.hash, lb.height(), last,
                          params().member());
            }
        } else {
            var builder = Executions.newBuilder();
            txns.forEach(e -> builder.addExecutions(e));

            var next = new HashedBlock(params().digestAlgorithm(),
                                       view.produce(lb.height() + 1, lb.hash, builder.build()));
            previousBlock.set(next);

            final var validation = view.generateValidation(next);
            ds.offer(validation);
            final var p = new PendingBlock(next, new HashMap<>(), new AtomicBoolean());
            pending.put(next.hash, p);
            p.witnesses.put(params().member(), validation);
            log.debug("Created block: {} height: {} prev: {} last: {} on: {}", next.hash, next.height(), lb.hash, last,
                      params().member());
        }
        if (last) {
            started.set(true);
            transitions.lastBlock();
        }
    }

    private Digest getViewId() {
        return view.context().getId();
    }

    private void newEpoch(Integer epoch) {
        log.trace("new epoch: {} on: {}", epoch, params().member());
        if (epoch == 0) {
            produceAssemble();
        }
        transitions.newEpoch(epoch, lastEpoch);
    }

    private Parameters params() {
        return view.params();
    }

    private void produceAssemble() {
        final var vlb = previousBlock.get();
        nextViewId = vlb.hash;
        nextAssembly.addAll(Committee.viewMembersOf(nextViewId, params().context()));
        final var assemble = new HashedBlock(params().digestAlgorithm(), view.produce(vlb.height()
        + 1, vlb.hash, Assemble.newBuilder().setNextView(vlb.hash.toDigeste()).build()));
        previousBlock.set(assemble);
        final var validation = view.generateValidation(assemble);
        final var p = new PendingBlock(assemble, new HashMap<>(), new AtomicBoolean());
        pending.put(assemble.hash, p);
        p.witnesses.put(params().member(), validation);
        ds.offer(validation);
        log.debug("View assembly: {} block: {} height: {} body: {} from: {} on: {}", nextViewId, assemble.hash,
                  assemble.height(), assemble.block.getBodyCase(), getViewId(), params().member());
    }

    private void publish(PendingBlock p) {
        log.debug("Published pending: {} height: {} on: {}", p.block.hash, p.block.height(), params().member());
        p.published.set(true);
        pending.remove(p.block.hash);
        final var cb = CertifiedBlock.newBuilder()
                                     .setBlock(p.block.block)
                                     .addAllCertifications(p.witnesses.values()
                                                                      .stream()
                                                                      .map(v -> v.getWitness())
                                                                      .toList())
                                     .build();
        view.publish(new HashedCertifiedBlock(params().digestAlgorithm(), cb));
    }

    private PendingBlock validate(Validate v) {
        Digest hash = Digest.from(v.getHash());
        var p = pending.get(hash);
        if (p == null) {
            return null;
        }
        if (!view.validate(p.block, v)) {
            log.trace("Invalid validate for: {} on: {}", hash, params().member());
            return null;
        }
        p.witnesses.put(view.context().getMember(Digest.from(v.getWitness().getId())), v);
        return p;
    }
}
