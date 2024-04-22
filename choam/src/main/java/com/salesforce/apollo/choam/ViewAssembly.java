/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Reconfiguration;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Reconfigure;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Transitions;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.proto.Digeste;
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

import static com.salesforce.apollo.choam.ViewContext.print;
import static com.salesforce.apollo.cryptography.QualifiedBase64.publicKey;
import static com.salesforce.apollo.cryptography.QualifiedBase64.signature;

/**
 * View reconfiguration. Attempts to create a new view reconfiguration. The protocol comes to an agreement on the
 * underlying Context membership view diadem and thus the membership of the next assembly.  Members propose their
 * current views and reach 2/3 agreement on the diadem.  The next assembly members are contacted to retrieve their view
 * keys and signed commitments on such.  Protocol terminates after this next slate of members stabilizes or the last
 * epoch occurs.
 *
 * @author hal.hildebrand
 */
public class ViewAssembly {

    private final static Logger                            log           = LoggerFactory.getLogger(ViewAssembly.class);
    protected final      Transitions                       transitions;
    private final        AtomicBoolean                     cancelSlice   = new AtomicBoolean();
    private final        Digest                            nextViewId;
    private final        Map<Digest, Views>                viewProposals = new ConcurrentHashMap<>();
    private final        Map<Digest, SignedViewMember>     proposals     = new ConcurrentHashMap<>();
    private final        Consumer<Assemblies>              publisher;
    private final        Map<Digest, Join>                 slate         = new HashMap<>();
    private final        ViewContext                       view;
    private final        CommonCommunications<Terminal, ?> comms;
    private final        Set<Digest>                       polled        = Collections.newSetFromMap(
    new ConcurrentSkipListMap<>());
    private volatile     View                              selected;

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
        log.debug("View reconfiguration from: {} to: {} on: {}", view.context().getId(), nextViewId,
                  params().member().getId());
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
        if (selected == null) {
            log.info("Cannot complete view assembly: {} as selected is null on: {}", nextViewId,
                     params().member().getId());
            transitions.failed();
            return;
        }
        cancelSlice.set(true);
        // Fill out the proposals with the unreachable members of the next assembly
        Sets.difference(selected.assembly.keySet(), proposals.keySet())
            .forEach(m -> proposals.put(m, SignedViewMember.newBuilder()
                                                           .setVm(ViewMember.newBuilder()
                                                                            .setId(m.toDigeste())
                                                                            .setView(nextViewId.toDigeste()))
                                                           .build()));
        log.debug("View Assembly: {} completed with: {} members on: {}", nextViewId, slate.size(),
                  params().member().getId());
    }

    void finalElection() {
        transitions.complete();
    }

    Consumer<List<Assemblies>> inbound() {
        return lre -> lre.forEach(this::assemble);
    }

    private void assemble(Assemblies ass) {
        log.info("Assembling {} joins and {} views on: {}", ass.getJoinsCount(), ass.getViewsCount(),
                 params().member().getId());
        ass.getJoinsList().stream().filter(view::validate).forEach(sj -> join(sj.getJoin(), false));
        if (selected != null) {
            return;
        }
        for (SignedViews svs : ass.getViewsList()) {
            if (view.validate(svs)) {
                log.info("Adding views: {} from: {}",
                         svs.getViews().getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList(),
                         Digest.from(svs.getViews().getMember()));
                viewProposals.put(Digest.from(svs.getViews().getMember()), svs.getViews());
            } else {
                log.info("Invalid views: {} from: {} on: {}",
                         svs.getViews().getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList(),
                         Digest.from(svs.getViews().getMember()), params().member().getId());
            }
        }
        vote();
    }

    private Map<Digest, Member> assemblyOf(List<Digeste> committee) {
        var last = view.pendingViews().last();
        return committee.stream()
                        .map(d -> last.context().getMember(Digest.from(d)))
                        .collect(Collectors.toMap(m -> m.getId(), m -> m));
    }

    private void castVote() {
        var views = view.pendingViews()
                        .getViews(nextViewId)
                        .setMember(params().member().getId().toDigeste())
                        .setVid(nextViewId.toDigeste())
                        .build();
        log.info("Voting for: {} on: {}", nextViewId, params().member().getId());
        publisher.accept(Assemblies.newBuilder()
                                   .addViews(
                                   SignedViews.newBuilder().setViews(views).setSignature(view.sign(views).toSig()))
                                   .build());
    }

    private void castVote(Views vs, List<com.salesforce.apollo.choam.proto.View> majorities,
                          Multiset<com.salesforce.apollo.choam.proto.View> consensus) {
        var ordered = vs.getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList();
        var lastIndex = -1;
        com.salesforce.apollo.choam.proto.View last = null;
        for (var v : majorities) {
            var i = ordered.indexOf(Digest.from(v.getDiadem()));
            if (i != -1) {
                if (i > lastIndex) {
                    last = v;
                    lastIndex = i;
                }
            }
        }
        if (last != null) {
            consensus.add(last);
        }
    }

    private void completeSlice(Duration retryDelay, AtomicReference<Runnable> reiterate) {
        if (gathered()) {
            return;
        }

        log.trace("Proposal incomplete of: {} polled: {}, total: {}  retrying: {} on: {}", nextViewId,
                  polled.stream().sorted().toList(), selected.assembly.size(), retryDelay, params().member().getId());
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
        log.debug("Join reply: {} on: {}", print(signedViewMember, params().digestAlgorithm()),
                  params().member().getId());
        join(signedViewMember, true);
        return !gathered();
    }

    private boolean gathered() {
        if (polled.size() == selected.assembly.size()) {
            log.trace("Polled all +");
            cancelSlice.set(true);
            return true;
        }
        return false;
    }

    private void join(SignedViewMember svm, boolean direct) {
        final var mid = Digest.from(svm.getVm().getId());
        final var m = selected.assembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return;
        }
        var viewId = Digest.from(svm.getVm().getView());
        if (!nextViewId.equals(viewId)) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view id for member: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Join of: {} on: {}", print(svm, params().digestAlgorithm()), params().member().getId());
        }

        if (!m.verify(JohnHancock.from(svm.getSignature()), svm.getVm().toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid signature for view member: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return;
        }

        PubKey encoded = svm.getVm().getConsensusKey();

        if (!m.verify(signature(svm.getVm().getSignature()), encoded.toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Could not verify consensus key from view member: {} on: {}",
                          print(svm, params().digestAlgorithm()), params().member().getId());
            }
            return;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            if (log.isTraceEnabled()) {
                log.trace("Could not deserialize consensus key from view member: {} on: {}",
                          print(svm, params().digestAlgorithm()), params().member().getId());
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
                    log.trace("Publishing view member: {} sig: {} on: {}", print(svm, params().digestAlgorithm()),
                              params().digestAlgorithm().digest(signature.toSig().toByteString()),
                              params().member().getId());
                }
            } else if (log.isTraceEnabled()) {
                log.trace("Adding discovered view member: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
        }
    }

    private Parameters params() {
        return view.params();
    }

    private void vote() {
        Multiset<com.salesforce.apollo.choam.proto.View> candidates = HashMultiset.create();
        viewProposals.values().forEach(v -> candidates.addAll(v.getViewsList()));
        var majority = params().majority();
        var majorities = candidates.entrySet()
                                   .stream()
                                   .filter(e -> e.getCount() >= majority)
                                   .map(e -> e.getElement())
                                   .toList();
        if (majorities.isEmpty()) {
            log.info("No majority views: {} on: {}", candidates.entrySet()
                                                               .stream()
                                                               .map(e -> "%s:%s".formatted(e.getCount(), Digest.from(
                                                               e.getElement().getDiadem())))
                                                               .toList(), params().member().getId());
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace("Majority views: {} on: {}", majorities.stream().map(v -> Digest.from(v.getDiadem())).toList(),
                      params().member().getId());
        }
        Multiset<com.salesforce.apollo.choam.proto.View> consensus = HashMultiset.create();
        viewProposals.values().forEach(vs -> {
            castVote(vs, majorities, consensus);
        });
        var ratification = consensus.entrySet()
                                    .stream()
                                    .filter(e -> e.getCount() >= majority)
                                    .map(Multiset.Entry::getElement)
                                    .sorted(Comparator.comparing(view -> Digest.from(view.getDiadem())))
                                    .collect(Collectors.toList());
        if (consensus.isEmpty()) {
            log.debug("No consensus views on: {}", params().member().getId());
            return;
        } else if (log.isTraceEnabled()) {
            log.debug("Consensus views: {} on: {}", ratification.stream().map(v -> Digest.from(v.getDiadem())).toList(),
                      params().member().getId());
        }
        var winner = ratification.getFirst();
        selected = new View(Digest.from(winner.getDiadem()), assemblyOf(winner.getCommitteeList()));
        if (log.isDebugEnabled()) {
            log.debug("Selected: {} on: {}", selected, params().member().getId());
        }
        transitions.viewDetermined();
    }

    private record View(Digest diadem, Map<Digest, Member> assembly) {
        public View(CHOAM.PendingView view, Digest viewId) {
            this(view.diadem(), Committee.viewMembersOf(viewId, view.context())
                                         .stream()
                                         .collect(Collectors.toMap(Member::getId, m -> m)));

        }

        @Override
        public String toString() {
            return "View{" + "diadem=" + diadem + ", assembly=" + assembly.keySet().stream().sorted().toList() + '}';
        }
    }

    private class Recon implements Reconfiguration {

        @Override
        public void certify() {
            if (proposals.size() == selected.assembly.size()) {
                cancelSlice.set(true);
                log.debug("Certifying: {} required: {} of: {} slate: {}  on: {}", nextViewId, selected.assembly.size(),
                          nextViewId, proposals.keySet().stream().sorted().toList(), params().member().getId());
                transitions.certified();
            } else {
                log.debug("Not certifying: {} required: {} slate: {} of: {} on: {}", nextViewId,
                          selected.assembly.size(), proposals.entrySet().stream().sorted().toList(), nextViewId,
                          params().member().getId());
            }
        }

        @Override
        public void complete() {
            ViewAssembly.this.complete();
        }

        @Override
        public void elect() {
            if (selected != null && proposals.size() == selected.assembly().size()) {
                proposals.entrySet().forEach(e -> slate.put(e.getKey(), joinOf(e.getValue())));
                cancelSlice.set(true);
                log.debug("Electing view: {} slate: {} on: {}", nextViewId, slate.keySet().stream().sorted().toList(),
                          params().member().getId());
                transitions.complete();
            } else {
                log.error("Failed election, selected: {} required: {} slate: {} of: {} on: {}", selected != null,
                          view.context().getRingCount(), proposals.keySet().stream().sorted().toList(), nextViewId,
                          params().member().getId());
                transitions.failed();
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
            assert selected != null : "Have not selected a view";
            log.trace("Gathering assembly for: {} on: {}", nextViewId, params().member().getId());
            AtomicReference<Runnable> reiterate = new AtomicReference<>();
            var retryDelay = Duration.ofMillis(10);
            reiterate.set(() -> {
                var slice = new ArrayList<>(selected.assembly.values());
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
            ViewAssembly.this.castVote();
        }

        private Join joinOf(SignedViewMember vm) {
            return Join.newBuilder().setMember(vm).build();
        }
    }
}
