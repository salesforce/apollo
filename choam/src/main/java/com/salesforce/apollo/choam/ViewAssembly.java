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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reassemble;
import com.salesfoce.apollo.choam.proto.Reassemble.AssemblyCase;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.Validations;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.choam.proto.ViewMembers;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Reconfiguration;
import com.salesforce.apollo.choam.fsm.Reconfigure;
import com.salesforce.apollo.choam.support.OneShot;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * View reconfiguration. Attempts to create a new view reconfiguration. View
 * reconfiguration needs 2f+1 certified members from the next view.
 * <p>
 * This implementation uses Ethereal and requires two epochs:
 * <ul>
 * <li>Epoch 0: gathering of next view members</li>
 * <li>Epoch 1: certifying of next view members</li>
 * </ul>
 * At the end of the last epoch, the protol finishes with a list of 2f+1 Joins
 * with 2f+1 certifications from the current view, or fails
 * 
 * @author hal.hildebrand
 *
 */
public class ViewAssembly implements Reconfiguration {

    record AJoin(Member m, Join j) {}

    private record Proposed(ViewMember vm, Member member, Map<Member, Validate> validations) {}

    private final static Logger log = LoggerFactory.getLogger(ViewAssembly.class);

    protected volatile OneShot          ds;
    protected final Map<Digest, Member> nextAssembly;
    protected final Digest              nextViewId;
    protected final Transitions         transitions;
    protected final ViewContext         view;

    private volatile Thread               blockingThread;
    private final SliceIterator<Terminal> committee;
    private final Ethereal                controller;
    private final ChRbcGossip             coordinator;
    private final Map<Digest, Proposed>   proposals = new ConcurrentHashMap<>();
    private final Map<Member, Join>       slate     = new ConcurrentHashMap<>();
    private final AtomicBoolean           started   = new AtomicBoolean();

    public ViewAssembly(Digest nextViewId, ViewContext vc, CommonCommunications<Terminal, ?> comms) {
        view = vc;
        this.nextViewId = nextViewId;
        ds = new OneShot();
        nextAssembly = Committee.viewMembersOf(nextViewId, params().context())
                                .stream()
                                .collect(Collectors.toMap(m -> m.getId(), m -> m));
        committee = new SliceIterator<Terminal>("Committee for " + nextViewId, params().member(),
                                                new ArrayList<>(nextAssembly.values()), comms, params().exec());
        // Create a new context for reconfiguration
        final Digest reconPrefixed = view.context().getId().xor(nextViewId);
        Context<Member> reContext = new ContextImpl<Member>(reconPrefixed, view.context().memberCount(),
                                                            view.context().getProbabilityByzantine(),
                                                            view.context().getBias());
        reContext.activate(view.context().activeMembers());

        final Fsm<Reconfiguration, Transitions> fsm = Fsm.construct(this, Transitions.class, getStartState(), true);
        this.transitions = fsm.getTransitions();
        fsm.setName("View Recon" + params().member().getId());

        Config.Builder config = params().producer().ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        var remapped = CHOAM.rosterMap(reContext, reContext.activeMembers());
        short pid = 0;
        for (Digest d : remapped.keySet().stream().sorted().toList()) {
            if (remapped.get(d).equals(params().member())) {
                break;
            }
            pid++;
        }
        config.setPid(pid).setnProc((short) view.roster().size());
        config.setEpochLength(7).setNumberOfEpochs(3);
        config.setLabel("View Recon" + nextViewId + " on: " + params().member().getId());
        controller = new Ethereal(config.build(), params().producer().maxBatchByteSize(), dataSource(),
                                  (preblock, last) -> process(preblock, last), epoch -> {
                                      log.trace("Next epoch: {} state: {} on: {}", epoch,
                                                transitions.fsm().getCurrentState(), params().member().getId());
                                      transitions.nextEpoch(epoch);
                                  });
        coordinator = new ChRbcGossip(reContext, params().member(), controller.processor(), params().communications(),
                                      params().exec(),
                                      params().metrics() == null ? null : params().metrics().getReconfigureMetrics());

        log.debug("View Assembly from: {} to: {} recontext: {} next assembly: {} on: {}", view.context().getId(),
                  nextViewId, reContext.getId(), nextAssembly.keySet(), params().member().getId());
    }

    @Override
    public void certify() {
        ds = new OneShot();
        log.debug("Certifying Join proposals of: {} count: {} state: {} on: {}", nextViewId, proposals.size(),
                  transitions.fsm().getCurrentState(), params().member());
        ds.setValue(Reassemble.newBuilder()
                              .setValidations(Validations.newBuilder()
                                                         .addAllValidations(proposals.values().stream().map(p -> {
                                                             return view.generateValidation(p.vm);
                                                         }).toList()))
                              .build()
                              .toByteString());
    }

    @Override
    public void complete() {
        log.debug("View Assembly: {} completed with: {} members state: {} on: {}", nextViewId, slate.size(),
                  transitions.fsm().getCurrentState(), params().member().getId());
        transitions.complete();
    }

    @Override
    public void elect() {
        proposals.values()
                 .stream()
                 .filter(p -> p.validations.size() >= params().majority())
                 .sorted(Comparator.comparing(p -> p.member.getId()))
                 .forEach(p -> slate.put(p.member(), joinOf(p)));
        if (slate.size() >= params().majority()) {
            log.debug("Electing slate: {} of: {} state: {} on: {}", slate.size(), nextViewId,
                      transitions.fsm().getCurrentState(), params().member());
            transitions.complete();
        } else {
            log.debug("Failed to elect slate: {} of: {} state: {} on: {}", slate.size(), nextViewId,
                      transitions.fsm().getCurrentState(), params().member().getId());
            transitions.failed();
        }
    }

    @Override
    public void failed() {
        log.error("Failed view assembly for: {} state: {} on: {}", nextViewId, transitions.fsm().getCurrentState(),
                  params().member());
        stop();
    }

    @Override
    public void gather() {
        log.trace("Gathering assembly for: {} state: {} on: {}", nextViewId, transitions.fsm().getCurrentState(),
                  params().member());
        JoinRequest request = JoinRequest.newBuilder()
                                         .setContext(params().context().getId().toDigeste())
                                         .setNextView(nextViewId.toDigeste())
                                         .build();
        AtomicBoolean proceed = new AtomicBoolean(true);
        AtomicReference<Runnable> reiterate = new AtomicReference<>();
        AtomicInteger countDown = new AtomicInteger(1); // 1 rounds of attempts
        AtomicReference<Duration> retryDelay = new AtomicReference<>(Duration.ofMillis(10));
        reiterate.set(() -> committee.iterate((term, m) -> {
            if (proposals.containsKey(m.getId())) {
                return null;
            }
            log.trace("Requesting Join from: {} state: {} on: {}", term.getMember().getId(),
                      transitions.fsm().getCurrentState(), params().member().getId());
            return term.join(request);
        }, (futureSailor, term, m) -> consider(futureSailor, term, m, proceed),
                                              () -> completeSlice(retryDelay, proceed, reiterate, countDown)));
        reiterate.get().run();
    }

    public Map<Member, Join> getSlate() {
        final var c = slate;
        return c;
    }

    @Override
    public void nominate() {
        log.debug("Nominating proposal for: {} members: {} state: {} on: {}", nextViewId,
                  proposals.values().stream().map(p -> p.member.getId()).toList(), transitions.fsm().getCurrentState(),
                  params().member().getId());
        ds.setValue(Reassemble.newBuilder()
                              .setViewMembers(ViewMembers.newBuilder()
                                                         .addAllMembers(proposals.values()
                                                                                 .stream()
                                                                                 .map(p -> p.vm)
                                                                                 .toList())
                                                         .build())
                              .build()
                              .toByteString());
    }

    public Parameters params() {
        return view.params();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        coordinator.start(params().producer().gossipDuration(), params().scheduler());
        controller.start();
        transitions.fsm().enterStartState();
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.trace("Stopping view assembly: {} state: {} on: {}", nextViewId, transitions.fsm().getCurrentState(),
                  params().member());
        coordinator.stop();
        controller.stop();
        final var cur = blockingThread;
        blockingThread = null;
        if (cur != null) {
            cur.interrupt();
        }
    }

    protected Reconfigure getStartState() {
        return Reconfigure.AWAIT_ASSEMBLY;
    }

    protected void validate(Validate v) {
        final var cid = Digest.from(v.getWitness().getId());
        var certifier = view.context().getMember(cid);
        if (certifier == null) {
            log.warn("Unknown certifier: {} state: {} on: {}", cid, transitions.fsm().getCurrentState(),
                     params().member());
            return; // do not have the join yet
        }
        final var hash = Digest.from(v.getHash());
        final var member = nextAssembly.get(hash);
        if (member == null) {
            log.warn("Unknown validation member: {} state: {} on: {}", hash, transitions.fsm().getCurrentState(),
                     params().member());
            return;
        }
        var proposed = proposals.get(hash);
        if (proposed == null) {
            log.warn("Invalid certification, unknown view join: {} state: {} on: {}", hash,
                     transitions.fsm().getCurrentState(), params().member().getId());
            return; // do not have the join yet
        }
        if (!view.validate(proposed.vm, v)) {
            log.warn("Invalid cetification for view join: {} state: {} from: {} on: {}", hash,
                     Digest.from(v.getWitness().getId()), transitions.fsm().getCurrentState(),
                     params().member().getId());
            return;
        }
        proposed.validations.put(certifier, v);
        log.debug("Validation of view member: {}:{} using certifier: {} state: {} on: {}", member.getId(), hash,
                  certifier.getId(), transitions.fsm().getCurrentState(), params().member().getId());
    }

    void assembled() {
        transitions.assembled();
    }

    private void completeSlice(AtomicReference<Duration> retryDelay, AtomicBoolean proceed,
                               AtomicReference<Runnable> reiterate, AtomicInteger countDown) {
        final var count = proposals.size();
        if (count == nextAssembly.size()) {
            proceed.set(false);
            log.trace("Proposal assembled: {} state: {} on: {}", nextViewId, transitions.fsm().getCurrentState(),
                      params().member().getId());
            transitions.gathered();
            return;
        }

        final var delay = retryDelay.get();
        if (delay.compareTo(params().producer().maxGossipDelay()) < 0) {
            retryDelay.accumulateAndGet(Duration.ofMillis(100), (a, b) -> a.plus(b));
        }

        if (count >= params().majority()) {
            if (countDown.decrementAndGet() >= 0) {
                log.trace("Retrying, attempting full assembly of: {} gathered: {} desired: {} delay: {} state: {} on: {}",
                          nextViewId, proposals.keySet().stream().toList(), nextAssembly.size(), delay,
                          transitions.fsm().getCurrentState(), params().member().getId());
                proceed.set(true);
                params().scheduler().schedule(() -> reiterate.get().run(), delay.toMillis(), TimeUnit.MILLISECONDS);
                return;
            }
            proceed.set(false);
            log.trace("Proposal assembled: {} gathered: {} out of: {} state: {} on: {}", nextViewId, count,
                      nextAssembly.size(), transitions.fsm().getCurrentState(), params().member().getId());
            transitions.gathered();
            return;
        }

        log.trace("Proposal incomplete of: {} gathered: {} required: {}, retrying: {} state: {} on: {}", nextViewId,
                  proposals.keySet().stream().toList(), params().majority() + 1, delay,
                  transitions.fsm().getCurrentState(), params().member().getId());
        proceed.set(true);
        params().scheduler().schedule(() -> reiterate.get().run(), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean consider(Optional<ListenableFuture<ViewMember>> futureSailor, Terminal term, Member m,
                             AtomicBoolean proceed) {
        if (futureSailor.isEmpty()) {
            return proceed.get();
        }
        ViewMember member;
        try {
            member = futureSailor.get().get();
            log.debug("Join reply from: {} state: {} on: {}", term.getMember().getId(),
                      transitions.fsm().getCurrentState(), params().member().getId());
        } catch (InterruptedException e) {
            log.debug("Error join response from: {} state: {} on: {}", term.getMember().getId(),
                      transitions.fsm().getCurrentState(), params().member().getId(), e);
            return proceed.get();
        } catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                if (!sre.getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
                    log.debug("Error join response from: {} state: {} on: {}", term.getMember().getId(),
                              transitions.fsm().getCurrentState(), params().member().getId(), sre);
                }
            } else {
                log.trace("Error join response from: {} state: {} on: {}", term.getMember().getId(),
                          transitions.fsm().getCurrentState(), params().member().getId(), e.getCause());
            }
            return proceed.get();
        }
        if (member.equals(ViewMember.getDefaultInstance())) {
            log.debug("Empty join response from: {} state: {} on: {}", term.getMember().getId(),
                      transitions.fsm().getCurrentState(), params().member().getId());
            return proceed.get();
        }
        var vm = new Digest(member.getId());
        if (!m.getId().equals(vm)) {
            log.debug("Invalid join response from: {} expected: {} state: {} on: {}", term.getMember().getId(), vm,
                      transitions.fsm().getCurrentState(), params().member().getId());
            return proceed.get();
        }
        log.debug("Adding delegate to: {} from: {} state: {} on: {}", getViewId(), term.getMember().getId(),
                  transitions.fsm().getCurrentState(), params().member().getId());
        join(member);

        return proceed.get();
    }

    private DataSource dataSource() {
        return new DataSource() {
            @Override
            public ByteString getData() {
                if (!started.get()) {
                    return ByteString.EMPTY;
                }
                try {
                    blockingThread = Thread.currentThread();
                    log.trace("Waiting for data state: {} on: {}", transitions.fsm().getCurrentState(),
                              params().member().getId());
                    final var take = ds.get();
                    log.trace("Data received: {}, state: {} proceeding on: {}", take.size(),
                              transitions.fsm().getCurrentState(), params().member().getId());
                    return take;
                } finally {
                    blockingThread = null;
                }
            }
        };
    }

    private Digest getViewId() {
        return coordinator.getContext().getId();
    }

    private void join(ViewMember vm) {
        final var mid = Digest.from(vm.getId());
        final var m = nextAssembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} state: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                          transitions.fsm().getCurrentState(), params().member().getId());
            }
            return;
        }

        PubKey encoded = vm.getConsensusKey();

        if (!m.verify(signature(vm.getSignature()), encoded.toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Could not verify consensus key from view member: {} state: {} on: {}",
                          ViewContext.print(vm, params().digestAlgorithm()), transitions.fsm().getCurrentState(),
                          params().member().getId());
            }
            return;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            if (log.isTraceEnabled()) {
                log.trace("Could not deserialize consensus key from view member: {} state: {} on: {}",
                          ViewContext.print(vm, params().digestAlgorithm()), transitions.fsm().getCurrentState(),
                          params().member().getId());
            }
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace("Valid view member: {} state: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                      transitions.fsm().getCurrentState(), params().member().getId());
        }
        proposals.computeIfAbsent(mid, k -> new Proposed(vm, m, new ConcurrentHashMap<>()));
    }

    private Join joinOf(Proposed candidate) {
        final List<Certification> witnesses = candidate.validations.values()
                                                                   .stream()
                                                                   .map(v -> v.getWitness())
                                                                   .sorted(Comparator.comparing(c -> new Digest(c.getId())))
                                                                   .collect(Collectors.toList());
        return Join.newBuilder()
                   .setMember(candidate.vm)
                   .setView(nextViewId.toDigeste())
                   .addAllEndorsements(witnesses)
                   .build();
    }

    private void process(PreBlock preblock, boolean last) {
        log.trace("Preblock last: {} state: {} on: {}", last, transitions.fsm().getCurrentState(),
                  params().member().getId());
        preblock.data().stream().map(e -> {
            try {
                final var parseFrom = Reassemble.parseFrom(e);
                return parseFrom;
            } catch (InvalidProtocolBufferException ex) {
                log.trace("Error parsing Reassemble state: {} on: {}", transitions.fsm().getCurrentState(),
                          params().member().getId());
                return (Reassemble) null;
            }
        }).filter(e -> e != null).forEach(re -> process(re));
        if (last) {
            transitions.complete();
        }
    }

    private void process(Reassemble re) {
        if (re.getAssemblyCase() != AssemblyCase.ASSEMBLY_NOT_SET) {
            log.trace("Processing: {} state: {} on: {}", re.getAssemblyCase(), transitions.fsm().getCurrentState(),
                      params().member().getId());
        }
        switch (re.getAssemblyCase()) {
        case VIEWMEMBERS:
            re.getViewMembers().getMembersList().forEach(e -> join(e));
            break;
        case VALIDATIONS:
            re.getValidations().getValidationsList().stream().forEach(e -> validate(e));
            break;
        default:
            break;
        }
    }
}
