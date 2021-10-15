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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Joins;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Reconfiguration;
import com.salesforce.apollo.choam.fsm.Reconfigure;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.utils.BbBackedInputStream;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
abstract public class ViewAssembly implements Reconfiguration {
    private final static Logger log = LoggerFactory.getLogger(ViewAssembly.class);

    private final static String RECONFIGURE_PREFIX = "Reconfiguration View";

    protected final ReliableBroadcaster coordinator;
    protected final Digest              nextViewId;
    protected final Transitions         transitions;
    protected final ViewContext         view;

    private final List<Join>              assembled = new CopyOnWriteArrayList<>();
    private final SliceIterator<Terminal> committee;
    private final Controller              controller;
    private final Map<Member, Join>       joins     = new ConcurrentHashMap<>();
    private final Set<Member>             nextAssembly;

    public ViewAssembly(Digest nextViewId, ViewContext vc, CommonCommunications<Terminal, ?> comms) {
        view = vc;
        this.nextViewId = nextViewId;
        nextAssembly = Committee.viewMembersOf(nextViewId, params().context());
        committee = new SliceIterator<Terminal>("Committee for " + nextViewId, params().member(),
                                                new ArrayList<>(nextAssembly), comms, params().dispatcher());
        // Create a new context for reconfiguration
        final Digest reconPrefixed = view.context().getId().prefix(RECONFIGURE_PREFIX);
        Context<Member> reContext = new Context<Member>(reconPrefixed, 0.33, view.context().activeMembers().size());
        reContext.activate(view.context().activeMembers());

        coordinator = new ReliableBroadcaster(params().producer().coordination().clone().setMember(params().member())
                                                      .setContext(reContext).build(),
                                              params().communications());
        coordinator.registerHandler((id, msgs) -> msgs.forEach(msg -> process(msg)));

        Config.Builder config = params().producer().ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        Short pid = view.roster().get(params().member().getId());
        if (pid == null) {
            config.setPid((short) 0).setnProc((short) 1);
        } else {
            config.setPid(pid).setnProc((short) view.roster().size());
        }
        config.setEpochLength(4).setNumberOfEpochs(1);

        final RoundScheduler roundScheduler = new RoundScheduler(reContext.timeToLive());
        coordinator.register(i -> roundScheduler.tick(i));
        controller = new Ethereal().deterministic(config.build(), dataSource(),
                                                  (preblock, last) -> create(preblock, last),
                                                  preUnit -> broadcast(preUnit));

        final Fsm<Reconfiguration, Transitions> fsm = Fsm.construct(this, Transitions.class, Reconfigure.GATHER, true);
        fsm.setName("View Recon" + params().member().getId());
        this.transitions = fsm.getTransitions();
        log.info("View Reconfiguration: {} committee: {} from: {} to: {} next assembly: {} roster: {} pid: {} on: {}",
                 reContext.getId(), reContext.activeMembers(), view.context().getId(), nextViewId, nextAssembly,
                 view.roster(), pid, params().member());
    }

    @Override
    public void complete() {
        controller.stop();
        coordinator.stop();
        log.info("Assembly: {} completed on: {}", nextViewId, params().member());
    }

    @Override
    public void continueValidating() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void convene() {
        log.debug("Convening assembly of: {} on: {}", nextViewId, params().member());

        controller.start();
        coordinator.start(params().producer().gossipDuration(), params().scheduler());
    }

    @Override
    public void failed() {
        coordinator.stop();
        controller.stop();
    }

    public void gatherAssembly() {
        coordinator.start(params().producer().gossipDuration(), params().scheduler());
        JoinRequest request = JoinRequest.newBuilder().setContext(params().context().getId().toDigeste())
                                         .setNextView(nextViewId.toDigeste()).build();
        AtomicBoolean proceed = new AtomicBoolean(true);
        AtomicReference<Runnable> reiterate = new AtomicReference<>();
        AtomicInteger countDown = new AtomicInteger(3); // 3 rounds of attempts
        reiterate.set(Utils.wrapped(() -> committee.iterate((term, m) -> {
            log.trace("Requesting Join from: {} on: {}", term.getMember().getId(), params().member());
            return term.join(request);
        }, (futureSailor, term, m) -> consider(futureSailor, term, m, proceed),
                                                            () -> completeSlice(proceed, reiterate, countDown)),
                                    log));
        reiterate.get().run();
    }

    public void start() {
        transitions.fsm().enterStartState();
    }

    @Override
    public void validation(Validate validate) {
        throw new UnsupportedOperationException();
    }

    protected abstract void assembled(Map<Member, Join> aggregate);

    protected Parameters params() {
        return view.params();
    }

    protected boolean process(Digest sender, Coordinate coordination) {
        if (coordination.hasUnit()) {
            Short source = view.roster().get(sender);
            if (source == null) {
                log.debug("No pid in roster: {} matching: {} on: {}", view.roster(), sender, params().member());
                if (params().metrics() != null) {
                    params().metrics().invalidSourcePid();
                }
                return true;
            }
            publish(sender, source, coordination.getUnit());
            return true;
        }
        return false;
    }

    private void assemble() {
        log.debug("Attempting assembly of: {} assembled: {} on: {}", nextViewId, assembled.size(), params().member());

        final int toleranceLevel = params().toleranceLevel();
        final HashMultimap<Member, Join> proposed = assembled.stream()
                                                             .filter(j -> nextViewId.equals(new Digest(j.getView())))
                                                             .filter(j -> params().context()
                                                                                  .getMember(new Digest(j.getMember()
                                                                                                         .getId())) != null)
                                                             .collect(Multimaps.toMultimap(j -> params().context()
                                                                                                        .getMember(new Digest(j.getMember()
                                                                                                                               .getId())),
                                                                                           j -> j,
                                                                                           () -> HashMultimap.create()));
        log.debug("Aggregate of: {} proposed: {} on: {}", nextViewId, proposed.size(), params().member());

        final Map<Member, Join> reduced = proposed.asMap().entrySet().stream()
                                                  .collect(Collectors.toMap(e -> e.getKey(),
                                                                            e -> reduce(e.getKey(), e.getValue())));
        log.debug("Aggregate of: {} reduced: {} on: {}", nextViewId, reduced.size(), params().member());

        var aggregate = reduced.entrySet().stream()
                               .filter(e -> e.getValue().getEndorsementsList().size() > toleranceLevel)
                               .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        log.debug("Aggregate of: {} joins: {} on: {}", nextViewId, aggregate.size(), params().member());
        assembled(aggregate);
    }

    private void broadcast(Unit unit) {
        if (params().metrics() != null) {
            params().metrics().broadcast(unit);
        }
        log.trace("Broadcasting: {} for: {} on: {}", unit, getViewId(), params().member());
        coordinator.publish(Coordinate.newBuilder().setUnit(unit.toPreUnit_s()).build());
    }

    private void completeSlice(AtomicBoolean proceed, AtomicReference<Runnable> reiterate, AtomicInteger countDown) {
        if (joins.size() == nextAssembly.size()) {
            proceed.set(false);
            log.trace("Assembled: {} on: {}", nextViewId, params().member());
            transitions.assembled();
        } else if (countDown.decrementAndGet() >= 0) {
            log.trace("Retrying assembly of: {} on: {}", nextViewId, params().member());
            reiterate.get().run();
        } else if (joins.size() > params().toleranceLevel()) {
            log.trace("Assembled: {} with: {} on: {}", nextViewId, joins.size(), params().member());
            transitions.assembled();
        } else {
            log.trace("Failing assembly of: {} gathered: {} on: {}", nextViewId, joins.size(), params().member());
            proceed.set(false);
            transitions.failed();
        }
    }

    private boolean consider(Optional<ListenableFuture<ViewMember>> futureSailor, Terminal term, Member m,
                             AtomicBoolean proceed) {

        if (futureSailor.isEmpty()) {
            return true;
        }
        ViewMember member;
        try {
            member = futureSailor.get().get();
            log.debug("Join reply from: {} on: {}", term.getMember().getId(), params().member().getId());
        } catch (InterruptedException e) {
            log.debug("Error join response from: {} on: {}", term.getMember().getId(), params().member().getId(), e);
            return proceed.get();
        } catch (ExecutionException e) {
            log.debug("Error join response from: {} on: {}", term.getMember().getId(), params().member().getId(),
                      e.getCause());
            return proceed.get();
        }
        if (member.equals(ViewMember.getDefaultInstance())) {
            log.debug("Empty join response from: {} on: {}", term.getMember().getId(), params().member().getId());
            return proceed.get();
        }
        var vm = new Digest(member.getId());
        if (!m.getId().equals(vm)) {
            log.debug("Invalid join response from: {} expected: {} on: {}", term.getMember().getId(), vm,
                      params().member().getId());
            return proceed.get();
        }

        PubKey encoded = member.getConsensusKey();

        if (!term.getMember().verify(signature(member.getSignature()), encoded.toByteString())) {
            log.debug("Could not verify consensus key from join: {} on: {}", term.getMember().getId(),
                      params().member());
            return proceed.get();
        }
        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            log.debug("Could not deserialize consensus key from: {} on: {}", term.getMember().getId(),
                      params().member());
            return proceed.get();
        }
        JohnHancock signed = params().member().sign(encoded.toByteString());
        if (signed == null) {
            log.debug("Could not sign consensus key from: {} on: {}", term.getMember().getId(), params().member());
            return proceed.get();
        }
        log.debug("Adding delegate to: {} from: {} on: {}", getViewId(), term.getMember().getId(), params().member());
        joins.put(term.getMember(),
                  Join.newBuilder().setMember(member).setView(nextViewId.toDigeste())
                      .addEndorsements(Certification.newBuilder().setId(params().member().getId().toDigeste())
                                                    .setSignature(signed.toSig()))
                      .build());
        return proceed.get();
    }

    private void create(PreBlock preblock, boolean last) {
        preblock.data().stream().map(e -> {
            log.debug("Creating preblock: {} last: {} on: {}",
                      params().digestAlgorithm().digest(BbBackedInputStream.aggregate(preblock.data())), last,
                      params().member());
            try {
                return Joins.parseFrom(e);
            } catch (InvalidProtocolBufferException ex) {
                log.trace("Error parsing joins on: {}", params().member());
                return (Joins) null;
            }
        }).filter(e -> e != null).forEach(e -> assembled.addAll(e.getJoinsList()));
        if (last) {
            assemble();
        }
    }

    private DataSource dataSource() {
        return new DataSource() {
            @Override
            public ByteString getData() {
                var data = Joins.newBuilder().addAllJoins(joins.values()).build().toByteString();

                if (params().metrics() != null) {
                    params().metrics().publishedBatch(joins.size(), data.size());
                }
                log.trace("Join txn: {} joins totalling: {} bytes pid: {} on: {}", joins.size(), data.size(),
                          view.roster().get(params().member().getId()), params().member());
                return data;
            }
        };
    }

    private Digest getViewId() {
        return coordinator.getContext().getId();
    }

    private void process(Msg msg) {
        Coordinate coordination;
        try {
            coordination = Coordinate.parseFrom(msg.content());
        } catch (InvalidProtocolBufferException e) {
            log.debug("Error deserializing from: {} on: {}", msg.source(), params().member());
            if (params().metrics() != null) {
                params().metrics().coordDeserialError();
            }
            return;
        }
        log.trace("Received msg from: {} type: {} on: {}", msg.source(), coordination.getMsgCase(), params().member());
        if (params().metrics() != null) {
            params().metrics().incTotalMessages();
        }
        process(msg.source(), coordination);
    }

    private void publish(Digest member, Short source, PreUnit_s preUnit) {
        final Controller current = controller;
        PreUnit pu = PreUnit.from(preUnit, params().digestAlgorithm());
        if (pu.creator() != source) {
            log.trace("Received: {} invalid source pid: {} from member: {} on: {}", pu, source, member,
                      params().member());
            return;
        }
        log.trace("Received: {} source pid: {} member: {} on: {}", pu, source, member, params().member());
        current.input().accept(source, pu);
    }

    private Join reduce(Member member, Collection<Join> js) {
        var max = js.stream().map(j -> validate(member, j)).filter(j -> j != null)
                    .collect(Multimaps.toMultimap(j -> j.getMember().getConsensusKey(), j -> j,
                                                  () -> HashMultimap.create()))
                    .asMap().entrySet().stream()
                    .max((a, b) -> Integer.compare(a.getValue().size(), b.getValue().size()));

        var proto = max.isEmpty() ? null : max.get().getValue().stream().reduce((a, b) -> {
            a.addAllEndorsements(b.getEndorsementsList());
            return a;
        }).get();
        List<Certification> endorsements = new ArrayList<>(proto.getEndorsementsList());
        proto.clearEndorsements();
        endorsements.sort(Comparator.comparing(c -> new Digest(c.getId())));
        proto.addAllEndorsements(endorsements);
        return proto.build();
    }

    private boolean validate(Certification c, PubKey encoded) {
        var d = new Digest(c.getId());
        Member m = params().context().getMember(d);
        if (m == null) {
            log.debug("Invalid certifier: {} on: {}", d, params().member());
            return false;
        }
        var validated = m.verify(JohnHancock.from(c.getSignature()), encoded.toByteString());
        if (!validated) {
            log.debug("Could not validate consensus key: {} using member: {} on: {}",
                      params().digestAlgorithm().digest(encoded.toByteString()), m.getId(), params().member());
        }
        return validated;
    }

    private Join.Builder validate(Member m, Join j) {
        ViewMember member = j.getMember();

        PubKey encoded = member.getConsensusKey();

        if (!m.verify(signature(member.getSignature()), encoded.toByteString())) {
            log.debug("Could not verify consensus key from: {} on: {}", m.getId(), params().member());
            return null;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            log.debug("Could not deserialize consensus key from: {} on: {}", m.getId(), params().member());
            return null;
        }
        return Join.newBuilder(j).clearEndorsements()
                   .addAllEndorsements(j.getEndorsementsList().stream().filter(c -> validate(c, encoded)).toList());
    }
}
