/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Driven;
import com.salesforce.apollo.choam.fsm.Driven.Earner;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.choam.proto.SubmitResult.Result;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.TxDataSource;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Config.Builder;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.membership.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An "Earner"
 *
 * @author hal.hildebrand
 */
public class Producer {

    private static final Logger                            log                = LoggerFactory.getLogger(Producer.class);
    private final        AtomicBoolean                     assembled          = new AtomicBoolean();
    private final        AtomicReference<ViewAssembly>     assembly           = new AtomicReference<>();
    private final        AtomicReference<HashedBlock>      checkpoint         = new AtomicReference<>();
    private final        CommonCommunications<Terminal, ?> comms;
    private final        Ethereal                          controller;
    private final        ChRbcGossip                       coordinator;
    private final        TxDataSource                      ds;
    private final        int                               lastEpoch;
    private final        Set<Member>                       nextAssembly       = new HashSet<>();
    private final        Map<Digest, PendingBlock>         pending            = new ConcurrentSkipListMap<>();
    private final        BlockingQueue<Reassemble>         pendingReassembles = new LinkedBlockingQueue<>();
    private final        Map<Digest, List<Validate>>       pendingValidations = new ConcurrentSkipListMap<>();
    private final        AtomicReference<HashedBlock>      previousBlock      = new AtomicReference<>();
    private final        AtomicBoolean                     reconfigured       = new AtomicBoolean();
    private final        AtomicBoolean                     started            = new AtomicBoolean(false);
    private final        Transitions                       transitions;
    private final        ViewContext                       view;
    private volatile     Digest                            nextViewId;

    public Producer(ViewContext view, HashedBlock lastBlock, HashedBlock checkpoint,
                    CommonCommunications<Terminal, ?> comms, String label) {
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
        final var blocks = ep.getEpochLength() - 2;
        final int maxElements = blocks * lastEpoch;

        ds = new TxDataSource(params.member(), maxElements, params.metrics(), producerParams.maxBatchByteSize(),
                              producerParams.batchInterval(), producerParams.maxBatchCount(),
                              params().drainPolicy().build());

        log.info("Producer max elements: {} reconfiguration epoch: {} on: {}", maxElements, lastEpoch,
                 params.member().getId());

        var fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName("Producer%s on: %s".formatted(getViewId(), params.member().getId()));
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
                                  transitions::create, this::newEpoch, label);
        coordinator = new ChRbcGossip(view.context(), params().member(), controller.processor(),
                                      params().communications(), producerMetrics);
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

    private void create(List<ByteString> preblock, boolean last) {
        if (log.isDebugEnabled()) {
            log.debug("emit last: {} preblock: {} on: {}", last,
                      preblock.stream().map(DigestAlgorithm.DEFAULT::digest).toList(), params().member().getId());
        }
        var aggregate = preblock.stream().map(e -> {
            try {
                return UnitData.parseFrom(e);
            } catch (InvalidProtocolBufferException ex) {
                log.error("Error parsing unit data on: {}", params().member().getId(), ex);
                return null;
            }
        }).filter(Objects::nonNull).toList();

        aggregate.stream()
                 .flatMap(e -> e.getValidationsList().stream())
                 .map(this::validate)
                 .filter(Objects::nonNull)
                 .filter(p -> !p.published.get())
                 .filter(p -> p.witnesses.size() >= params().majority())
                 .forEach(this::publish);

        var reass = Reassemble.newBuilder();
        aggregate.stream()
                 .flatMap(e -> e.getReassembliesList().stream())
                 .forEach(r -> reass.addAllMembers(r.getMembersList()).addAllValidations(r.getValidationsList()));
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

        HashedBlock lb = previousBlock.get();
        final var txns = aggregate.stream().flatMap(e -> e.getTransactionsList().stream()).toList();

        if (!txns.isEmpty()) {
            log.trace("transactions: {} comb hash: {} height: {} on: {}", txns.size(), txns.stream()
                                                                                           .map(t -> CHOAM.hashOf(t,
                                                                                                                  params().digestAlgorithm()))
                                                                                           .reduce(Digest::xor)
                                                                                           .orElse(null),
                      lb.height().add(1), params().member().getId());
            var builder = Executions.newBuilder();
            txns.forEach(builder::addExecutions);

            var next = new HashedBlock(params().digestAlgorithm(),
                                       view.produce(lb.height().add(1), lb.hash, builder.build(), checkpoint.get()));
            previousBlock.set(next);

            final var validation = view.generateValidation(next);
            ds.offer(validation);
            final var p = new PendingBlock(next, new HashMap<>(), new AtomicBoolean());
            pending.put(next.hash, p);
            p.witnesses.put(params().member(), validation);
            log.debug("Produced block: {} hash: {} height: {} prev: {} last: {} on: {}", next.block.getBodyCase(),
                      next.hash, next.height(), lb.hash, last, params().member().getId());
            processPendingValidations(next, p);
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

    private void processPendingValidations(HashedBlock block, PendingBlock p) {
        var pending = pendingValidations.remove(block.hash);
        if (pending != null) {
            pending.forEach(v -> validate(v, p, block.hash));
            if (p.witnesses.size() >= params().majority()) {
                publish(p);
            }
        }
    }

    private void produceAssemble() {
        final var vlb = previousBlock.get();
        nextViewId = vlb.hash;
        nextAssembly.addAll(Committee.viewMembersOf(nextViewId, view.pendingView()));
        log.debug("Assembling: {} on: {}", nextViewId, params().member().getId());
        final var assemble = new HashedBlock(params().digestAlgorithm(), view.produce(vlb.height().add(1), vlb.hash,
                                                                                      Assemble.newBuilder()
                                                                                              .setNextView(
                                                                                              vlb.hash.toDigeste())
                                                                                              .build(),
                                                                                      checkpoint.get()));
        previousBlock.set(assemble);
        final var validation = view.generateValidation(assemble);
        final var p = new PendingBlock(assemble, new HashMap<>(), new AtomicBoolean());
        pending.put(assemble.hash, p);
        p.witnesses.put(params().member(), validation);
        ds.offer(validation);
        log.debug("Produced block: {} hash: {} height: {} from: {} on: {}", assemble.block.getBodyCase(), assemble.hash,
                  assemble.height(), getViewId(), params().member().getId());
        processPendingValidations(assemble, p);
    }

    private void publish(PendingBlock p) {
        assert p.witnesses.size() >= params().majority() : "Publishing non majority block";
        log.debug("Published pending: {} hash: {} height: {} witnesses: {} on: {}", p.block.block.getBodyCase(),
                  p.block.hash, p.block.height(), p.witnesses.values().size(), params().member().getId());
        p.published.set(true);
        final var cb = CertifiedBlock.newBuilder()
                                     .setBlock(p.block.block)
                                     .addAllCertifications(
                                     p.witnesses.values().stream().map(Validate::getWitness).toList())
                                     .build();
        view.publish(new HashedCertifiedBlock(params().digestAlgorithm(), cb));
    }

    private PendingBlock validate(Validate v) {
        Digest hash = Digest.from(v.getHash());
        var p = pending.get(hash);
        if (p == null) {
            pendingValidations.computeIfAbsent(hash, h -> new CopyOnWriteArrayList<>()).add(v);
            return null;
        }
        return validate(v, p, hash);
    }

    private PendingBlock validate(Validate v, PendingBlock p, Digest hash) {
        if (!view.validate(p.block, v)) {
            log.trace("Invalid validate for: {} hash: {} on: {}", p.block.block.getBodyCase(), hash,
                      params().member().getId());
            return null;
        }
        p.witnesses.put(view.context().getMember(Digest.from(v.getWitness().getId())), v);
        return p;
    }

    record PendingBlock(HashedBlock block, Map<Member, Validate> witnesses, AtomicBoolean published) {
    }

    /** Leaf action Driven coupling for the Earner FSM */
    private class DriveIn implements Driven {

        @Override
        public void assembled() {
            if (!reconfigured.compareAndSet(false, true)) {
                log.debug("assembly already complete on: {}", params().member().getId());
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
            //            controller.completeIt();
            log.info("Produced: {} hash: {} height: {} slate: {} on: {}", reconfiguration.block.getBodyCase(),
                     reconfiguration.hash, reconfiguration.height(),
                     slate.keySet().stream().map(m -> m.getId()).sorted().toList(), params().member().getId());
            processPendingValidations(reconfiguration, p);
        }

        @Override
        public void checkAssembly() {
            ds.drain();
            final var dropped = ds.getRemainingTransactions();
            if (dropped != 0) {
                log.warn("Dropped txns: {} on: {}", dropped, params().member().getId());
            }
            final var viewAssembly = assembly.get();
            if (viewAssembly == null) {
                log.error("Assemble block never processed on: {}", params().member().getId());
                transitions.failed();
                return;
            }
            viewAssembly.finalElection();
            log.debug("Final view assembly election on: {}", params().member().getId());
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
            log.info("Produced: {} hash: {} height: {} for: {} on: {}", next.block.getBodyCase(), next.hash,
                     next.height(), getViewId(), params().member().getId());
            processPendingValidations(next, p);
            transitions.checkpointed();
        }

        @Override
        public void complete() {
            stop();
        }

        @Override
        public void create(List<ByteString> preblock, boolean last) {
            Producer.this.create(preblock, last);
        }

        @Override
        public void fail() {
            stop();
            view.onFailure();
        }

        @Override
        public void produceAssemble() {
            Producer.this.produceAssemble();
        }

        @Override
        public void reconfigure() {
            log.debug("Starting view reconfiguration: {} on: {}", nextViewId, params().member().getId());
            assembly.set(new ViewAssembly(nextViewId, view, Producer.this::addReassemble, comms) {
                @Override
                public void complete() {
                    super.complete();
                    log.debug("View reconfiguration: {} gathered: {} complete on: {}", nextViewId, getSlate().size(),
                              params().member().getId());
                    assembled.set(true);
                    Producer.this.transitions.viewComplete();
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
            coordinator.start(params().producer().gossipDuration());
        }
    }
}
