/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
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
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Join.Builder;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Joins;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
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
import com.salesforce.apollo.ethereal.Data.PreBlock;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
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
        private SliceIterator<Terminal>         committee;
        private final Map<Member, Join.Builder> joins     = new HashMap<>();
        private Set<Member>                     nextAssembly;
        private int                             principal = 0;

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
                if (!nextViewId.equals(id)) {
                    log.debug("Invalid view id: {}  on: {}", id, params.member());
                    continue;
                }
                Digest mid = new Digest(join.getMember().getId());
                Member delegate = coordinator.getContext().getActiveMember(mid);
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
                }
            }
            attemptAssembly();
        }

        @Override
        public void awaitView() {
            // TODO Auto-generated method stub

        }

        @Override
        public void cancelTimer(String label) {
            roundScheduler.cancel(label);
        }

        @Override
        public void convene() {
            // three attempts
            convene(new AtomicInteger(3));
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
        public void gatherAssembly() {
            nextAssembly = Committee.viewMembersOf(nextViewId, params.context());
            nextAssembly.forEach(m -> joins.put(m, Join.newBuilder().setView(nextViewId.toDigeste())));
            coordinator.start(params.gossipDuration(), params.scheduler());
            JoinRequest request = JoinRequest.newBuilder().setContext(params.context().getId().toDigeste())
                                             .setNextView(nextViewId.toDigeste()).build();
            AtomicBoolean proceed = new AtomicBoolean(true);
            AtomicReference<Runnable> reiterate = new AtomicReference<>();
            AtomicInteger countDown = new AtomicInteger(3); // 3 rounds of attempts
            reiterate.set(Utils.wrapped(() -> committee.iterate((term,
                                                                 m) -> joins.get(m).hasMember() ? null
                                                                                                : term.join(request),
                                                                (futureSailor, term, m) -> consider(futureSailor, term,
                                                                                                    m, proceed),
                                                                () -> {
                                                                    if (completeSlice()) {
                                                                        proceed.set(false);
                                                                        transitions.assembled();
                                                                    } else if (countDown.decrementAndGet() >= 0) {
                                                                        reiterate.get().run();
                                                                    } else {
                                                                        transitions.assembled();
                                                                        proceed.set(false);
                                                                    }
                                                                }),
                                        log));
            reiterate.get().run();
        }

        @Override
        public void generateView() {
            // TODO Auto-generated method stub

        }

        @Override
        public void initialState() {
            establishPrincipal();
        }

        @Override
        public void reconfigure() {
            reconfigure(new AtomicInteger(3));
        }

        @Override
        public void reconfigure(Block reconfigure) {
            HashedBlock hb = new HashedBlock(params.digestAlgorithm(), reconfigure);
            Validate validation = generateValidation(hb.hash, hb.block);
            if (validation != null) {
                coordinator.publish(Coordinate.newBuilder().setValidate(validation).build().toByteArray());
            }
        }

        @Override
        public void reform() {
            // TODO Auto-generated method stub

        }

        @Override
        public void validation(Validate validate) {
            reconfiguration.addCertifications(validate.getWitness());
        }

        private void attemptAssembly() {
            int toleranceLevel = coordinator.getContext().toleranceLevel();
            if (joins.values().stream().filter(b -> b.getEndorsementsCount() > toleranceLevel)
                     .count() > toleranceLevel) {
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
            log.debug("Adding delegate to: {} from: {} on: {}", coordinator.getContext().getId(),
                      term.getMember().getId(), params.member());
            joins.get(term.getMember()).setMember(member)
                 .addEndorsements(Certification.newBuilder().setId(params.member().getId().toDigeste())
                                               .setSignature(signed.toSig()));
            return proceed.get();
        }

        private void convene(AtomicInteger countdown) {
            if (countdown.decrementAndGet() >= 0) {
                roundScheduler.schedule(Driven.RECONVENE, () -> convene(countdown), 1);
            }
            coordinator.publish(Coordinate.newBuilder()
                                          .setJoins(Joins.newBuilder()
                                                         .addAllJoins(joins.values().stream().filter(b -> b.hasMember())
                                                                           .map(b -> b.build()).toList()))
                                          .build().toByteArray());
        }

        private boolean isPrincipal() {
            return params.member().equals(principal());
        }

        private Member principal() {
            return coordinator.getContext().ring(0).get(principal);
        }

        private void reconfigure(AtomicInteger countdown) {
            if (isPrincipal()) {
                if (reconfiguration.getCertificationsCount() > coordinator.getContext().toleranceLevel()) {
                    publisher.accept(new HashedCertifiedBlock(params.digestAlgorithm(), reconfiguration.build()));
                    transitions.reconfigured();
                    return;
                }
                Map<Member, Join> joined = joins.entrySet().stream().filter(e -> e.getValue().hasMember())
                                                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));
                Block reconfigure = reconfigureBlock.apply(joined, nextViewId);
                Validate validation = generateValidation(nextViewId, reconfigure);
                reconfiguration.setBlock(reconfigure).addCertifications(validation.getWitness());
                coordinator.publish(Coordinate.newBuilder().setReconfigure(reconfigure).build().toByteArray());
                coordinator.publish(Coordinate.newBuilder().setValidate(validation).build().toByteArray());
            }
            if (countdown.decrementAndGet() >= 0) {
                roundScheduler.schedule(RECONFIGURE, () -> reconfigure(countdown), 1);
            } else {
                roundScheduler.schedule(RECONFIGURE, () -> transitions.failed(), 1);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final CommonCommunications<Terminal, ?>            comms;
    private final Controller                                   controller;
    private final ReliableBroadcaster                          coordinator;
    private final Ethereal                                     ethereal;
    private final Fsm<Driven, Transitions>                     fsm;
    private final SimpleChannel<Coordinate>                    linear;
    private Digest                                             nextViewId;
    private final Parameters                                   params;
    private final Deque<PreBlock>                              pending         = new LinkedList<>();
    private final Consumer<HashedCertifiedBlock>               publisher;
    private final CertifiedBlock.Builder                       reconfiguration = CertifiedBlock.newBuilder();
    private final BiFunction<Map<Member, Join>, Digest, Block> reconfigureBlock;
    private final Map<Digest, Short>                           roster          = new HashMap<>();
    private final RoundScheduler                               roundScheduler;
    private final Signer                                       signer;
    private final BlockingDeque<Transaction>                   transactions    = new LinkedBlockingDeque<>();
    private final Transitions                                  transitions;

    public Producer(nextView viewMember, ReliableBroadcaster coordinator, CommonCommunications<Terminal, ?> comms,
                    Parameters params, BiFunction<Map<Member, Join>, Digest, Block> reconfigureBlock,
                    Consumer<HashedCertifiedBlock> publisher) {
        assert comms != null && params != null;
        this.params = params;
        this.comms = comms;
        this.reconfigureBlock = reconfigureBlock;
        this.publisher = publisher;
        signer = new SignerImpl(0, viewMember.consensusKeyPair().getPrivate());

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

    public void regenerate() {
        transitions.regenerate();
    }

    public void start() {
        transitions.start();
    }

    void setNextViewId(Digest nextViewId) {
        this.nextViewId = nextViewId;
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
        case JOINS:
            transitions.joins(coordination.getJoins());
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

    private Validate generateValidation(Digest hash, Block block) {
        JohnHancock signature = signer.sign(params.digestAlgorithm().digest(block.getHeader().toByteString())
                                                  .toDigeste().toByteString());
        if (signature == null) {
            log.error("Unable to sign block: {} on: {}", hash, params.member());
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
            Short source = roster.get(msg.source());
            if (source == null) {
                log.debug("No pid in roster matching: {} on: {}", msg.source(), params.member());
                if (metrics() != null) {
                    metrics().invalidSourcePid();
                }
                return;
            }
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

}
