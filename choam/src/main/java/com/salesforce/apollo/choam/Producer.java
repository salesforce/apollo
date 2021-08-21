/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.support.HashedBlock.height;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.CHOAM.ReconfigureBlock;
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
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
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
        private volatile ViewReconfiguration reconfigure;

        @Override
        public void complete() {
            Producer.this.complete();
        }

        @Override
        public void epochEnd() {
            final HashedBlock lb = previousBlock.get();
            reconfigure = new ViewReconfiguration(lb.hash, view, lb, comms, reconfigureBlock);
            log.info("Consensus complete, next view: {} on: {}", lb.hash, params().member());
            reconfigure.start();
        }

        @Override
        public void startProduction() {
            log.debug("Starting production of: {} on: {}", getViewId(), params().member());
            coordinator.start(params().gossipDuration(), params().scheduler());
            controller.start();
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

        private void stop() {
            final ViewReconfiguration current = reconfigure;
            if (current == null) {
                return;
            }
            current.complete();
        }

    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final CommonCommunications<Terminal, ?>   comms;
    private final Controller                          controller;
    private final ReliableBroadcaster                 coordinator;
    private final DataSource                          ds;
    private final Ethereal                            ethereal;
    private final SimpleChannel<Coordinate>           linear;
    private final Map<Digest, CertifiedBlock.Builder> pending       = new ConcurrentHashMap<>();
    private final AtomicReference<HashedBlock>        previousBlock = new AtomicReference<>();
    private final Set<Digest>                         published     = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ReconfigureBlock                    reconfigureBlock;
    private final Transitions                         transitions;
    private final ViewContext                         view;

    public Producer(ViewContext view, ReliableBroadcaster coordinator, CommonCommunications<Terminal, ?> comms,
                    HashedBlock lastBlock, ReconfigureBlock reconfigureBlock) {
        assert view != null && comms != null;
        this.view = view;
        this.previousBlock.set(lastBlock);
        this.reconfigureBlock = reconfigureBlock;
        this.comms = comms;
        ds = new TxDataSource(view.params(),
                              view.params().maxBatchByteSize() * view.params().ethereal().getEpochLength());

        // Ethereal consensus
        ethereal = new Ethereal();

        // Reliable broadcast of both Units and Coordination messages between valid
        // members of this committee
        this.coordinator = coordinator;
        this.coordinator.registerHandler((ctx, msgs) -> msgs.forEach(msg -> process(msg)));

        var fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName(params().member().getId().toString());
        transitions = fsm.getTransitions();

        // buffer for coordination messages
        linear = new SimpleChannel<>("Publisher linear for: " + params().member(), 100);
        linear.consumeEach(coordination -> transitions.validate(coordination.getValidate()));

        Config.Builder config = params().ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        Short pid = view.roster().get(params().member().getId());
        if (pid == null) {
            config.setPid((short) 0).setnProc((short) 1);
        } else {
            log.trace("Pid: {} for: {} on: {}", pid, getViewId(), params().member());
            config.setPid(pid).setnProc((short) view.roster().size());
        }

        // Our handle on consensus
        controller = ethereal.deterministic(config.build(), ds, (preblock, last) -> create(preblock, last),
                                            preUnit -> broadcast(preUnit));
        assert controller != null : "Controller is null";

        log.debug("Roster for: {} is: {} on: {}", getViewId(), view.roster(), params().member());
    }

    public void complete() {
        log.debug("Closing producer for: {} on: {}", getViewId(), params().member());
        controller.stop();
        linear.close();
        coordinator.stop();
        ((DriveIn) transitions.context()).stop();
    }

    public void start() {
        log.info("Starting production for: {} on: {}", getViewId(), params().member());
        transitions.start();
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
        final HashedBlock lb = previousBlock.get();
        var next = new HashedBlock(params().digestAlgorithm(), view.produce(lb.height() + 1, lb.hash, builder.build()));
        previousBlock.set(next);
        var validation = view.generateValidation(next.hash, next.block);
        coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
        var cb = pending.computeIfAbsent(next.hash, h -> CertifiedBlock.newBuilder());
        cb.setBlock(next.block);
        cb.addCertifications(validation.getWitness());
        log.debug("Block: {} height: {} last: {} created on: {}", next.hash, next.height(), last, params().member());
        if (last) {
            transitions.drain();
        }
        maybePublish(next.hash, cb);
    }

    private Digest getViewId() {
        return coordinator.getContext().getId();
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
            transitions.publishedBlock();
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
        controller.input().accept(source, Collections.singletonList(pu));
    }
}
