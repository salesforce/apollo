/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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
import com.salesfoce.apollo.choam.proto.Reassemble;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Result;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.UnitData;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Driven;
import com.salesforce.apollo.choam.fsm.Driven.Earner;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.TxDataSource;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Config.Builder;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.membership.Member;

/**
 * An "Earner"
 * 
 * @author hal.hildebrand
 *
 */
public class Producer {

    record PendingBlock(HashedBlock block, Map<Member, Validate> witnesses, AtomicBoolean published) {}

    /** Leaf action Driven coupling for the Earner FSM */
    private class DriveIn implements Driven {

        @Override
        public void assembled() {
            if (!reconfigured.compareAndSet(false, true)) {
                return;
            }
            final var slate = assembly.get().getSlate();
            var reconfiguration = new HashedBlock(params().digestAlgorithm(),
                                                  view.reconfigure(slate, nextViewId, previousBlock.get(),
                                                                   checkpoint.get()));
            var validation = view.generateValidation(reconfiguration);
            final var p = new PendingBlock(reconfiguration, new HashMap<>(), new AtomicBoolean());
            pending.put(reconfiguration.hash, p);
            p.witnesses.put(params().member(), validation);
            ds.offer(validation);
            log.info("Reconfiguration block: {} height: {} produced on: {}", reconfiguration.hash,
                     reconfiguration.height(), params().member().getId());
        }

        @Override
        public void checkAssembly() {
            ds.drain();
            assembly.get().election();
            if (assembled.get()) {
                assembled();
            }
        }

        @Override
        public void checkpoint() {
            log.info("Generating checkpoint block on: {}", params().member().getId());
            Block ckpt = view.checkpoint();
            if (ckpt == null) {
                log.error("Cannot generate checkpoint block on: {}", params().member().getId());
                transitions.failed();
                return;
            }
            var next = new HashedBlock(params().digestAlgorithm(), ckpt);
            previousBlock.set(next);
            checkpoint.set(next);
            var validation = view.generateValidation(next);
            ds.offer(validation);
            final var p = new PendingBlock(next, new HashMap<>(), new AtomicBoolean());
            pending.put(next.hash, p);
            p.witnesses.put(params().member(), validation);
            log.info("Produced checkpoint: {} height: {} for: {} on: {}", next.hash, next.height(), getViewId(),
                     params().member().getId());
            transitions.checkpointed();
        }

        @Override
        public void complete() {
            stop();
        }

        @Override
        public void create(PreBlock preblock, boolean last) {
            Producer.this.create(preblock, last);
        }

        @Override
        public void fail() {
            stop();
        }

        @Override
        public void produceAssemble() {
            Producer.this.produceAssemble();
        }

        @Override
        public void reconfigure() {
            log.debug("Starting view reconfiguration for: {} on: {}", nextViewId, params().member().getId());
            assembly.set(new ViewAssembly(nextViewId, view, r -> addReassemble(r), comms) {
                @Override
                public void complete() {
                    log.debug("View reconfiguration: {} gathered: {} complete on: {}", nextViewId, getSlate().size(),
                              params().member().getId());
                    assembled.set(true);
                    Producer.this.transitions.viewComplete();
                    super.complete();
                }
            });
            assembly.get().start();
            assembly.get().assembled();
            List<Reassemble> reasses = new ArrayList<>();
            pendingReassembles.drainTo(reasses);
            assembly.get().inbound().accept(reasses);
        }

        @Override
        public void startProduction() {
            log.debug("Starting production for: {} on: {}", getViewId(), params().member().getId());
            controller.start();
            coordinator.start(params().producer().gossipDuration(), params().scheduler());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final AtomicBoolean                     assembled          = new AtomicBoolean();
    private final AtomicReference<ViewAssembly>     assembly           = new AtomicReference<>();
    private final AtomicReference<HashedBlock>      checkpoint         = new AtomicReference<>();
    private final CommonCommunications<Terminal, ?> comms;
    private final Ethereal                          controller;
    private final ChRbcGossip                       coordinator;
    private final TxDataSource                      ds;
    private final int                               lastEpoch;
    private final Set<Member>                       nextAssembly       = new HashSet<>();
    private volatile Digest                         nextViewId;
    private final Map<Digest, PendingBlock>         pending            = new ConcurrentHashMap<>();
    private final BlockingQueue<Reassemble>         pendingReassembles = new LinkedBlockingQueue<>();
    private final AtomicReference<HashedBlock>      previousBlock      = new AtomicReference<>();
    private final AtomicBoolean                     reconfigured       = new AtomicBoolean();
    private final AtomicBoolean                     started            = new AtomicBoolean(false);
    private final Transitions                       transitions;
    private final ViewContext                       view;

    public Producer(ViewContext view, HashedBlock lastBlock, HashedBlock checkpoint,
                    CommonCommunications<Terminal, ?> comms) {
        assert view != null;
        this.view = view;
        this.previousBlock.set(lastBlock);
        this.comms = comms;
        this.checkpoint.set(checkpoint);

        final Parameters params = view.params();
        final var producerParams = params.producer();
        final Builder ep = producerParams.ethereal();

        lastEpoch = ep.getNumberOfEpochs() - 1;

        // Number of rounds we can provide data for
        final var blocks = ep.getEpochLength() - 4;
        final int maxElements = blocks * lastEpoch;

        ds = new TxDataSource(params.member(), maxElements, params.metrics(), producerParams.maxBatchByteSize(),
                              producerParams.batchInterval(), producerParams.maxBatchCount());

        log.trace("Producer max elements: {} reconfiguration epoch: {} on: {}", maxElements, lastEpoch,
                  params.member().getId());

        var fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName("Producer" + getViewId() + params().member().getId().toString());
        transitions = fsm.getTransitions();

        Config.Builder config = params().producer().ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        Short pid = view.roster().get(params().member().getId());
        if (pid == null) {
            config.setPid((short) 0).setnProc((short) 1);
        } else {
            log.trace("Pid: {} for: {} on: {}", pid, getViewId(), params().member().getId());
            config.setPid(pid).setnProc((short) view.roster().size());
        }

        config.setLabel("Producer" + getViewId() + " on: " + params().member().getId());
        var producerMetrics = params().metrics() == null ? null : params().metrics().getProducerMetrics();
        controller = new Ethereal(config.build(), params().producer().maxBatchByteSize() + (8 * 1024), ds,
                                  (preblock, last) -> transitions.create(preblock, last), epoch -> newEpoch(epoch));
        coordinator = new ChRbcGossip(view.context(), params().member(), controller.processor(),
                                      params().communications(), params().exec(), producerMetrics);
        log.debug("Roster for: {} is: {} on: {}", getViewId(), view.roster(), params().member().getId());
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
        log.trace("Closing producer for: {} on: {}", getViewId(), params().member().getId());
        controller.stop();
        coordinator.stop();
        final var c = assembly.get();
        if (c != null) {
            c.stop();
        }
        ds.close();
    }

    public SubmitResult submit(Transaction transaction) {
        if (ds.offer(transaction)) {
            return SubmitResult.newBuilder().setResult(Result.PUBLISHED).build();
        } else {
            return SubmitResult.newBuilder().setResult(Result.BUFFER_FULL).build();
        }
    }

    private void addReassemble(Reassemble r) {
        log.trace("Adding reassembly members: {} validations: {} on: {}", r.getMembersCount(), r.getValidationsCount(),
                  params().member().getId());
        ds.offer(r);
    }

    private void create(PreBlock preblock, boolean last) {
        log.debug("preblock produced, last: {} on: {}", last, params().member().getId());
        var aggregate = preblock.data().stream().map(e -> {
            try {
                return UnitData.parseFrom(e);
            } catch (InvalidProtocolBufferException ex) {
                log.error("Error parsing unit data on: {}", params().member().getId());
                return (UnitData) null;
            }
        }).filter(e -> e != null).toList();

        aggregate.stream()
                 .flatMap(e -> e.getValidationsList().stream())
                 .map(witness -> validate(witness))
                 .filter(p -> p != null)
                 .filter(p -> !p.published.get())
                 .filter(p -> p.witnesses.size() >= params().majority())
                 .forEach(p -> publish(p));

        var reass = Reassemble.newBuilder();
        aggregate.stream().flatMap(e -> e.getReassembliesList().stream()).forEach(r -> {
            reass.addAllMembers(r.getMembersList()).addAllValidations(r.getValidationsList());
        });
        if (reass.getMembersCount() > 0 || reass.getValidationsCount() > 0) {
            final var ass = assembly.get();
            if (ass != null) {
                log.trace("Consuming reassemblies: {} members: {} validations: {} on: {}", aggregate.size(),
                          reass.getMembersCount(), reass.getValidationsCount(), params().member().getId());
                ass.inbound().accept(Collections.singletonList(reass.build()));
            } else {
                log.trace("Pending reassemblies: {} members: {} validations: {} on: {}", aggregate.size(),
                          reass.getMembersCount(), reass.getValidationsCount(), params().member().getId());
                pendingReassembles.add(reass.build());
            }
        }

        HashedBlock lb = previousBlock.get();
        final var txns = aggregate.stream().flatMap(e -> e.getTransactionsList().stream()).toList();

        if (!txns.isEmpty()) {
            log.warn("transactions: {} cum hash: {} height: {} on: {}", txns.size(),
                     txns.stream()
                         .map(t -> CHOAM.hashOf(t, params().digestAlgorithm()))
                         .reduce((a, b) -> a.xor(b))
                         .orElse(null),
                     lb.height().add(1), params().member().getId());
            var builder = Executions.newBuilder();
            txns.forEach(e -> builder.addExecutions(e));

            var next = new HashedBlock(params().digestAlgorithm(),
                                       view.produce(lb.height().add(1), lb.hash, builder.build(), checkpoint.get()));
            previousBlock.set(next);

            final var validation = view.generateValidation(next);
            ds.offer(validation);
            final var p = new PendingBlock(next, new HashMap<>(), new AtomicBoolean());
            pending.put(next.hash, p);
            p.witnesses.put(params().member(), validation);
            log.debug("Created block: {} height: {} prev: {} last: {} on: {}", next.hash, next.height(), lb.hash, last,
                      params().member().getId());
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
        log.trace("new epoch: {} on: {}", epoch, params().member().getId());
        transitions.newEpoch(epoch, lastEpoch);
    }

    private Parameters params() {
        return view.params();
    }

    private void produceAssemble() {
        final var vlb = previousBlock.get();
        nextViewId = vlb.hash;
        nextAssembly.addAll(Committee.viewMembersOf(nextViewId, params().context()));
        final var assemble = new HashedBlock(params().digestAlgorithm(),
                                             view.produce(vlb.height().add(1), vlb.hash,
                                                          Assemble.newBuilder()
                                                                  .setNextView(vlb.hash.toDigeste())
                                                                  .build(),
                                                          checkpoint.get()));
        previousBlock.set(assemble);
        final var validation = view.generateValidation(assemble);
        final var p = new PendingBlock(assemble, new HashMap<>(), new AtomicBoolean());
        pending.put(assemble.hash, p);
        p.witnesses.put(params().member(), validation);
        ds.offer(validation);
        log.debug("View assembly: {} block: {} height: {} body: {} from: {} on: {}", nextViewId, assemble.hash,
                  assemble.height(), assemble.block.getBodyCase(), getViewId(), params().member().getId());
    }

    private void publish(PendingBlock p) {
//        assert previousBlock.get().hash.equals(Digest.from(p.block.block.getHeader().getPrevious())) : "Pending block: "
//        + p.block.hash + " previous: " + Digest.from(p.block.block.getHeader().getPrevious()) + " is not: "
//        + previousBlock.get().hash;
        log.debug("Published pending: {} height: {} on: {}", p.block.hash, p.block.height(), params().member().getId());
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
            log.trace("Invalid validate for: {} on: {}", hash, params().member().getId());
            return null;
        }
        p.witnesses.put(view.context().getMember(Digest.from(v.getWitness().getId())), v);
        return p;
    }
}
