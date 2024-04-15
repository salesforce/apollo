/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.chiralbehaviors.tron.Fsm;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Reconfiguration;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Reconfigure;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Transitions;
import com.salesforce.apollo.choam.proto.Assemblies;
import com.salesforce.apollo.choam.proto.Join;
import com.salesforce.apollo.choam.proto.SignedJoin;
import com.salesforce.apollo.choam.proto.SignedViewMember;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.proto.PubKey;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.salesforce.apollo.cryptography.QualifiedBase64.publicKey;
import static com.salesforce.apollo.cryptography.QualifiedBase64.signature;

/**
 * View reconfiguration. Attempts to create a new view reconfiguration. View reconfiguration needs at least 2f+1
 * certified members from the next view. The protocol finishes with a list of at least 2f+1 Joins with at least 2f+1
 * certifications from the current view, or fails
 *
 * @author hal.hildebrand
 */
public class ViewAssembly {

    private final static Logger                            log         = LoggerFactory.getLogger(ViewAssembly.class);
    protected final      Transitions                       transitions;
    private final        AtomicBoolean                     cancelSlice = new AtomicBoolean();
    private final        Digest                            nextViewId;
    private final        Map<Digest, SignedViewMember>     proposals   = new ConcurrentHashMap<>();
    private final        Consumer<Assemblies>              publisher;
    private final        Map<Digest, Join>                 slate       = new ConcurrentSkipListMap<>();
    private final        ViewContext                       view;
    private final        CommonCommunications<Terminal, ?> comms;
    private final        Set<Digest>                       polled      = Collections.newSetFromMap(
    new ConcurrentSkipListMap<>());
    private volatile     Map<Digest, Member>               nextAssembly;

    public ViewAssembly(Digest nextViewId, ViewContext vc, Consumer<Assemblies> publisher,
                        CommonCommunications<Terminal, ?> comms) {
        view = vc;
        this.nextViewId = nextViewId;
        this.publisher = publisher;
        this.comms = comms;

        final Fsm<Reconfiguration, Transitions> fsm = Fsm.construct(new Recon(), Transitions.class,
                                                                    Reconfigure.AWAIT_ASSEMBLY, true);
        this.transitions = fsm.getTransitions();
        fsm.setName("View Assembly%s on: %s".formatted(nextViewId, params().member().getId()));

        nextAssembly = Committee.viewMembersOf(nextViewId, view.pendingView())
                                .stream()
                                .collect(Collectors.toMap(Member::getId, m -> m));
        log.debug("View reconfiguration from: {} to: {}, next assembly: {} on: {}", view.context().getId(), nextViewId,
                  nextAssembly.keySet(), params().member().getId());
    }

    public Map<Digest, Join> getSlate() {
        return slate;
    }

    public void start() {
        transitions.fsm().enterStartState();
    }

    public void stop() {
        cancelSlice.set(true);
    }

    void assembled() {
        transitions.assembled();
    }

    void complete() {
        cancelSlice.set(true);
        proposals.entrySet()
                 .stream()
                 .forEach(e -> slate.put(e.getKey(), Join.newBuilder().setMember(e.getValue()).build()));
        Context<Member> pendingContext = view.pendingView();
        log.debug("View Assembly: {} completed with: {} members on: {}", nextViewId, slate.size(),
                  params().member().getId());
    }

    void finalElection() {
        transitions.complete();
    }

    Consumer<List<Assemblies>> inbound() {
        return lre -> {
            lre.forEach(ass -> assemble(ass));
        };
    }

    private void assemble(Assemblies ass) {
        ass.getJoinsList().stream().filter(sj -> view.validate(sj)).forEach(sj -> join(sj.getJoin(), false));
        ass.getViewsList().stream().filter(sv -> view.validate(sv));
    }

    private void completeSlice(Duration retryDelay, AtomicReference<Runnable> reiterate) {
        if (gathered()) {
            return;
        }

        log.trace("Proposal incomplete of: {} polled: {}, total: {}  retrying: {} on: {}", nextViewId,
                  polled.stream().sorted().toList(), nextAssembly.size(), retryDelay, params().member().getId());
        if (!cancelSlice.get()) {
            Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory())
                     .schedule(() -> Thread.ofVirtual().start(Utils.wrapped(reiterate.get(), log)),
                               retryDelay.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private boolean consider(Optional<SignedViewMember> futureSailor, Terminal term, Member m) {
        if (futureSailor.isEmpty()) {
            return !gathered();
        }
        SignedViewMember signedViewMember;
        signedViewMember = futureSailor.get();
        if (signedViewMember.equals(SignedViewMember.getDefaultInstance())) {
            log.debug("Empty join response from: {} on: {}", term.getMember().getId(), params().member().getId());
            return !gathered();
        }
        var vm = new Digest(signedViewMember.getVm().getId());
        if (!m.getId().equals(vm)) {
            log.debug("Invalid join response from: {} expected: {} on: {}", term.getMember().getId(), vm,
                      params().member().getId());
            return !gathered();
        }
        log.debug("Join reply from: {} on: {}", term.getMember().getId(), params().member().getId());
        join(signedViewMember, true);
        return !gathered();
    }

    private boolean gathered() {
        if (polled.size() == nextAssembly.size()) {
            log.trace("Polled all +");
            cancelSlice.set(true);
            return true;
        }
        return false;
    }

    private void join(SignedViewMember svm, boolean direct) {
        final var mid = Digest.from(svm.getVm().getId());
        final var m = nextAssembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} on: {}", ViewContext.print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return;
        }
        var viewId = Digest.from(svm.getVm().getView());
        if (!nextViewId.equals(viewId)) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view id for member: {} on: {}", ViewContext.print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Join request from: {} vm: {} on: {}", m.getId(),
                      ViewContext.print(svm, params().digestAlgorithm()), params().member().getId());
        }

        if (!m.verify(JohnHancock.from(svm.getSignature()), svm.getVm().toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid signature for view member: {} on: {}",
                          ViewContext.print(svm, params().digestAlgorithm()), params().member().getId());
            }
            return;
        }

        PubKey encoded = svm.getVm().getConsensusKey();

        if (!m.verify(signature(svm.getVm().getSignature()), encoded.toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Could not verify consensus key from view member: {} on: {}",
                          ViewContext.print(svm, params().digestAlgorithm()), params().member().getId());
            }
            return;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            if (log.isTraceEnabled()) {
                log.trace("Could not deserialize consensus key from view member: {} on: {}",
                          ViewContext.print(svm, params().digestAlgorithm()), params().member().getId());
            }
            return;
        }
        polled.add(mid);
        if (proposals.putIfAbsent(mid, svm) == null) {
            if (direct) {
                var signature = view.sign(svm);
                publisher.accept(Assemblies.newBuilder()
                                           .addJoins(SignedJoin.newBuilder()
                                                               .setJoin(svm)
                                                               .setMember(params().member().getId().toDigeste())
                                                               .setSignature(signature.toSig())
                                                               .build())
                                           .build());
                if (log.isTraceEnabled()) {
                    log.trace("Publishing view member: {} sig: {} on: {}",
                              ViewContext.print(svm, params().digestAlgorithm()),
                              params().digestAlgorithm().digest(signature.toSig().toByteString()),
                              params().member().getId());
                }
            } else if (log.isTraceEnabled()) {
                log.trace("Adding discovered view member: {} on: {}",
                          ViewContext.print(svm, params().digestAlgorithm()), params().member().getId());
            }
        }
    }

    private Parameters params() {
        return view.params();
    }

    private record Proposed(SignedViewMember vm, Member member) {
    }

    private class Recon implements Reconfiguration {

        @Override
        public void certify() {
            if (proposals.size() == nextAssembly.size()) {
                cancelSlice.set(true);
                log.debug("Certifying: {} required: {} of: {} slate: {}  on: {}", proposals.size(), nextAssembly.size(),
                          nextViewId, proposals.keySet().stream().sorted().toList(), params().member().getId());
                transitions.certified();
            } else {
                log.debug("Not certifying: {} required: {} slate: {} of: {} on: {}", proposals.size(),
                          nextAssembly.size(), proposals.entrySet().stream().sorted().toList(), nextViewId,
                          params().member().getId());
            }
        }

        @Override
        public void complete() {
            ViewAssembly.this.complete();
        }

        @Override
        public void elect() {
            proposals.entrySet().stream().forEach(e -> slate.put(e.getKey(), joinOf(e.getValue())));
            if (slate.size() == view.pendingView().getRingCount()) {
                cancelSlice.set(true);
                log.debug("Electing: {} of: {} slate: {} on: {}", slate.size(), nextViewId,
                          slate.keySet().stream().sorted().toList(), params().member().getId());
                transitions.complete();
            } else {
                Context<Member> memberContext = view.pendingView();
                log.error("Failed election, required: {} slate: {} of: {} on: {}", view.pendingView().getRingCount(),
                          proposals.keySet().stream().sorted().toList(), nextViewId, params().member().getId());
            }
        }

        @Override
        public void failed() {
            stop();
            view.onFailure();
            log.debug("Failed view assembly for: {} on: {}", nextViewId, params().member().getId());
        }

        @Override
        public void gather() {
            log.trace("Gathering assembly for: {} on: {}", nextViewId, params().member().getId());
            AtomicReference<Runnable> reiterate = new AtomicReference<>();
            var retryDelay = Duration.ofMillis(10);
            reiterate.set(() -> {
                nextAssembly = Committee.viewMembersOf(nextViewId, view.pendingView())
                                        .stream()
                                        .collect(Collectors.toMap(Member::getId, m -> m));
                var slice = new ArrayList<>(nextAssembly.values());
                var committee = new SliceIterator<>("Committee for " + nextViewId, params().member(), slice, comms);
                committee.iterate((term, m) -> {
                    if (polled.contains(m.getId())) {
                        return null;
                    }
                    log.trace("Requesting Join from: {} on: {}", term.getMember().getId(), params().member().getId());
                    return term.join(nextViewId);
                }, ViewAssembly.this::consider, () -> completeSlice(retryDelay, reiterate), params().gossipDuration());
            });
            reiterate.get().run();
        }

        @Override
        public void nominate() {
            transitions.nominated();
        }

        @Override
        public void viewAgreement() {

        }

        private Join joinOf(SignedViewMember vm) {
            return Join.newBuilder().setMember(vm).build();
        }
    }
}
