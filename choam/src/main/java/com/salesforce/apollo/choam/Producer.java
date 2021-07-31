/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.fsm.Driven;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;
import com.salesforce.apollo.choam.fsm.Earner;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Data.PreBlock;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
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
        public void awaitView() {
            // TODO Auto-generated method stub

        }

        @Override
        public void establishPrincipal() {
            if (params.member().equals(principal())) {
                transitions.assumePrincipal();
            } else {
                transitions.assumeDelegate();
            }
        }

        @Override
        public void generateView() {
        }

        @Override
        public void initialState() {
            establishPrincipal();
        }

        @Override
        public void gatherAssembly() {
            // TODO Auto-generated method stub

        }

        @Override
        public void convene() {
            // TODO Auto-generated method stub
            
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final Controller                 controller;
    private final ReliableBroadcaster        coordinator;
    private final Ethereal                   ethereal;
    private final Fsm<Driven, Transitions>   fsm;
    private final SimpleChannel<Coordinate>  linear;
    private final Parameters                 params;
    private final Deque<PreBlock>            pending         = new LinkedList<>();
    private int                              principal       = 0;
    @SuppressWarnings("unused")
    private final CertifiedBlock.Builder     reconfiguration = CertifiedBlock.newBuilder();
    private final Map<Digest, Short>         roster          = new HashMap<>();
    private final BlockingDeque<Transaction> transactions    = new LinkedBlockingDeque<>();
    private final Transitions                transitions;

    public Producer(ReliableBroadcaster coordinator, Parameters params) {
        this.params = params;
        // Ethereal consensus
        ethereal = new Ethereal();

        // Reliable broadcast of both Units and Coordination messages between valid
        // members of this committee
        this.coordinator = coordinator;
        this.coordinator.registerHandler((ctx, msgs) -> msgs.forEach(msg -> process(msg)));

        // FSM driving this Earner
        fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName(params.member().getId().toString());
        transitions = fsm.getTransitions();

        // buffer for coordination messages
        linear = new SimpleChannel<>(100);
        linear.consumeEach(coordination -> coordinate(coordination));

        // Our handle on consensus
        controller = ethereal.deterministic(params.ethereal().clone().build(), dataSource(),
                                            preblock -> pending.add(preblock), preUnit -> broadcast(preUnit));
    }

    public void complete() {
        controller.stop();
        linear.close();
        coordinator.stop();
    }

    public void start() {
        transitions.start();
    }

    public void regenerate() {
        transitions.regenerate();
    }

    /**
     * Reliably broadcast this preUnit to all valid members of this committee
     */
    private void broadcast(PreUnit preUnit) {
        if (metrics() != null) {
            metrics().broadcast(preUnit);
        }
        coordinator.publish(Coordinate.newBuilder().setUnit(preUnit.toPreUnit_s()).build().toByteArray());
    }

    /**
     * Dispatch the coordination message through the FSM
     */
    private void coordinate(Coordinate coordination) {
        switch (coordination.getMsgCase()) {
        case PUBLISH:
            transitions.publish(coordination.getPublish());
            break;
        case RECONFIGURE:
            transitions.reconfigure(coordination.getReconfigure());
            break;
        case VALIDATE:
            transitions.validate(coordination.getValidate());
            break;
        default:
            break;
        }
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

    /**
     * The data to be used for a the next Unit produced by this Producer
     */
    private ByteString getData() {
        int bytesRemaining = params.maxBatchByteSize();
        int txnsRemaining = params.maxBatchSize();
        List<ByteString> batch = new ArrayList<>();
        while (txnsRemaining > 0 && transactions.peek() != null
        && bytesRemaining >= transactions.peek().getSerializedSize()) {
            txnsRemaining--;
            Transaction next = transactions.poll();
            bytesRemaining -= next.getSerializedSize();
            batch.add(next.toByteString());
        }
        int byteSize = params.maxBatchByteSize() - bytesRemaining;
        int batchSize = params.maxBatchSize() - txnsRemaining;
        log.info("Produced: {} txns totalling: {} bytes pid: {} on: {}", batchSize, byteSize,
                 roster.get(params.member().getId()), params.member());
        if (metrics() != null) {
            metrics().publishedBatch(batchSize, byteSize);
        }
        return ByteString.copyFrom(batch);
    }

    private ChoamMetrics metrics() {
        return params.metrics();
    }

    /**
     * Reliable broadcast message processing
     */
    private void process(Msg msg) {
        Short source = roster.get(msg.source());
        if (source == null) {
            log.debug("No pid in roster matching: {} on: {}", msg.source(), params.member());
            if (metrics() != null) {
                metrics().invalidSourcePid();
            }
            return;
        }
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
        log.debug("Received msg from: {} on: {}", msg.source(), params.member());
        if (metrics() != null) {
            metrics().incTotalMessages();
        }
        if (coordination.hasUnit()) {
            publish(msg.source(), source, PreUnit.from(coordination.getUnit(), params.digestAlgorithm()));
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
                      params.member());
            if (metrics() != null) {
                metrics().invalidUnit();
            }
            return;
        }
        controller.input().accept(source, Collections.singletonList(pu));
    }

    private Member principal() {
        return coordinator.getContext().ring(0).get(principal);
    }
}
