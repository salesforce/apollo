/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.fsm.Driven.PERIODIC_VALIDATIONS;
import static com.salesforce.apollo.choam.support.HashedBlock.height;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Assemble;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Outcome;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Driven;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;
import com.salesforce.apollo.choam.fsm.Earner;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.TxDataSource;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
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
import com.salesforce.apollo.utils.RoundScheduler;
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

        @Override
        public void checkpoint() {
            Block ckpt = blockProducer.checkpoint();
            if (ckpt == null) {
                log.error("Cannot generate checkpoint block on: {}", params().member());
                transitions.failed();
                return;
            }
            var next = new HashedBlock(params().digestAlgorithm(), ckpt);
            previousBlock.set(next);
            var validation = view.generateValidation(next);
            coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
            var cb = pending.computeIfAbsent(next.hash, h -> CertifiedBlock.newBuilder());
            cb.setBlock(next.block);
            cb.addCertifications(validation.getWitness());
            maybePublish(next.hash, cb);
            log.info("Produced checkpoint: {} for: {} on: {}", next.hash, getViewId(), params().member());
        }

        @Override
        public void complete() {
            Producer.this.complete();
        }

        @Override
        public void reconfigure() {
            log.debug("Attempting assembly of: {} assembled: {} on: {}", nextViewId, assembled.size(),
                      params().member());

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
            if (aggregate.size() > toleranceLevel) {
                var reconfigure = blockProducer.reconfigure(aggregate, nextViewId, previousBlock.get());
                var rhb = new HashedBlock(params().digestAlgorithm(), reconfigure);
                var validation = view.generateValidation(rhb);
                coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
                log.debug("Aggregate of: {} threshold reached: {} block: {} on: {}", nextViewId, aggregate.size(),
                          rhb.hash, params().member());
                var cb = pending.computeIfAbsent(rhb.hash, h -> CertifiedBlock.newBuilder());
                cb.setBlock(rhb.block);
                cb.addCertifications(validation.getWitness());
                maybePublish(rhb.hash, cb);
                log.debug("Reconfiguration block: {} height: {} created on: {}", rhb.hash, rhb.height(),
                          params().member());
            } else {
                log.warn("Aggregate of: {} threshold failed: {} required: {} on: {}", nextViewId, aggregate.size(),
                         toleranceLevel, params().member());
                transitions.failed();
            }
            periodicValidations(() -> transitions.lastBlock());
        }

        @Override
        public void startProduction() {
            log.info("Starting production of: {} on: {}", getViewId(), params().member());
            coordinator.start(params().producer().gossipDuration(), params().scheduler());
            final Controller current = controller;
            current.start();
        }

        private Join reduce(Member member, Collection<Join> js) {
            var max = js.stream().map(j -> Join.newBuilder(j)).filter(j -> j != null)
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
        public void cancelTimers() {
            scheduler.cancelAll();
        }
    }

    private class Recon {
        private final SliceIterator<Terminal> committee;
        private final Set<Member>             nextAssembly;

        private Recon() {
            nextAssembly = Committee.viewMembersOf(nextViewId, params().context());
            committee = new SliceIterator<Terminal>("Committee for " + nextViewId, params().member(),
                                                    new ArrayList<>(nextAssembly), comms, params().dispatcher());
        }

        private void completeSlice(AtomicBoolean proceed, AtomicReference<Runnable> reiterate,
                                   AtomicInteger countDown) {
            if (joins.size() == nextAssembly.size()) {
                proceed.set(false);
                log.trace("Assembled: {} on: {}", nextViewId, params().member());
            } else if (countDown.decrementAndGet() >= 0) {
                log.trace("Retrying assembly of: {} on: {}", nextViewId, params().member());
                reiterate.get().run();
            } else if (joins.size() > params().toleranceLevel()) {
                proceed.set(false);
                log.trace("Assembled: {} with: {} on: {}", nextViewId, joins.size(), params().member());
            } else {
                proceed.set(false);
                log.trace("Failing assembly of: {} gathered: {} on: {}", nextViewId, joins.size(), params().member());
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
                log.debug("Error join response from: {} on: {}", term.getMember().getId(), params().member().getId(),
                          e);
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
            log.debug("Adding delegate to: {} from: {} on: {}", getViewId(), term.getMember().getId(),
                      params().member());

            var j = joins.computeIfAbsent(m, k -> Join.newBuilder().setMember(member).setView(nextViewId.toDigeste()));
            j.addEndorsements(Certification.newBuilder().setId(params().member().getId().toDigeste())
                                           .setSignature(signed.toSig()));

            ds.submitJoin(j.build());
            return proceed.get();
        }

        private void gatherAssembly() {
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
    }

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static Join cannonical(Join.Builder proto) {
        List<Certification> endorsements = new ArrayList<>(proto.getEndorsementsList());
        proto.clearEndorsements();
        endorsements.sort(Comparator.comparing(c -> new Digest(c.getId())));
        proto.addAllEndorsements(endorsements);
        return proto.build();
    }

    private final List<Join>                          assembled     = new CopyOnWriteArrayList<>();
    private final BlockProducer                       blockProducer;
    private final AtomicBoolean                       closed        = new AtomicBoolean(false);
    private final CommonCommunications<Terminal, ?>   comms;
    private volatile Controller                       controller;
    private volatile ReliableBroadcaster              coordinator;
    private final TxDataSource                        ds;
    private final Map<Member, Join.Builder>           joins         = new ConcurrentHashMap<>();
    private final ExecutorService                     linear;
    private volatile Digest                           nextViewId;
    private final Map<Digest, CertifiedBlock.Builder> pending       = new ConcurrentHashMap<>();
    private final AtomicReference<HashedBlock>        previousBlock = new AtomicReference<>();
    private final Set<Digest>                         published     = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile Recon                            recon;
    private final AtomicInteger                       reconfigureCountdown;
    private volatile RoundScheduler                   scheduler;
    private final Transitions                         transitions;
    private final ViewContext                         view;

    public Producer(ViewContext view, HashedBlock lastBlock, BlockProducer blockProducer,
                    CommonCommunications<Terminal, ?> comms) {
        assert view != null;
        this.view = view;
        this.previousBlock.set(lastBlock);
        this.blockProducer = blockProducer;
        this.comms = comms;
        this.reconfigureCountdown = new AtomicInteger(30); // TODO params

        final Parameters params = view.params();
        final Builder ep = params.producer().ethereal();
        ds = new TxDataSource(params, (params.producer().maxBatchByteSize()
        * ((ep.getEpochLength() * ep.getNumberOfEpochs()) - 3)) * 2);
        final Context<Member> context = view.context();

        coordinator = new ReliableBroadcaster(params().producer().coordination().clone().setMember(params().member())
                                                      .setContext(context).build(),
                                              params().communications());
        coordinator.registerHandler((ctx, msgs) -> msgs.forEach(msg -> process(msg)));
        scheduler = new RoundScheduler(context.timeToLive());
        coordinator.register(i -> scheduler.tick(i));

        var fsm = Fsm.construct(new DriveIn(), Transitions.class, Earner.INITIAL, true);
        fsm.setName(params().member().getId().toString());
        transitions = fsm.getTransitions();

        // buffer for coordination messages
        linear = Executors.newSingleThreadExecutor();

        initializeConsensus();
    }

    public void complete() {
        log.info("Closing producer for: {} on: {}", getViewId(), params().member());
        final Controller current = controller;
        current.stop();
        linear.shutdown();
        coordinator.stop();
    }

    public Digest getNextViewId() {
        final Digest current = nextViewId;
        return current;
    }

    public void joins(List<Join> joins) {
        if (joins.isEmpty()) {
            return;
        }
        linear.execute(() -> {
            joins.forEach(join -> {
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
                    log.debug("Member not a committee member of: {} current: {} from: {} on: {}", view, getViewId(),
                              memberId, params().member());
                    return;
                }
                JohnHancock sig = JohnHancock.of(join.getMember().getSignature());
                if (!member.verify(sig, join.getMember().getConsensusKey().toByteString())) {
                    log.debug("Cannot validate consensus key for: {} current: {} from: {} on: {}", view, getViewId(),
                              memberId, params().member());
                    return;
                }
                log.debug("Adding join for: {} current: {} from: {} on: {}", view, getViewId(), memberId,
                          params().member());
                // looks good to me... First txn wins rule
                assembled.add(join);
            });
        });
    }

    public void start() {
        log.info("Starting production for: {} on: {}", getViewId(), params().member());
        transitions.start();
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
            log.info("Failure, cannot submit received txn: {} on: {}",
                     CHOAM.hashOf(transaction, params().digestAlgorithm()), params().member());
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

    private void initializeConsensus() {
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
                                            preUnit -> broadcast(preUnit));
        assert controller != null : "Controller is null";

        log.debug("Roster for: {} is: {} on: {}", getViewId(), view.roster(), params().member());
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
                var validation = view.generateValidation(new HashedBlock(params().digestAlgorithm(), b.getBlock()));
                coordinator.publish(Coordinate.newBuilder().setValidate(validation).build());
            });
            scheduler.schedule(PERIODIC_VALIDATIONS, action.get(), 1);
        });
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
        recon = new Recon();
        recon.gatherAssembly();
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
