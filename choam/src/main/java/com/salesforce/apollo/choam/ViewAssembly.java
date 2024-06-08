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
import com.salesforce.apollo.choam.fsm.Reconfiguration;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Reconfigure;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Transitions;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.proto.Digeste;
import com.salesforce.apollo.cryptography.proto.PubKey;
import com.salesforce.apollo.membership.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final static Logger                        log           = LoggerFactory.getLogger(ViewAssembly.class);
    protected final      Transitions                   transitions;
    private final        Digest                        nextViewId;
    private final        Map<Digest, Views>            viewProposals = new ConcurrentHashMap<>();
    private final        Map<Digest, SignedViewMember> proposals     = new ConcurrentHashMap<>();
    private final        Consumer<Assemblies>          publisher;
    private final        Map<Digest, Join>             slate         = new HashMap<>();
    private final        ViewContext                   view;
    private final        CompletableFuture<Vue>        onConsensus;
    private final        AtomicInteger                 countdown     = new AtomicInteger();
    private final        List<SignedViewMember>        pendingJoins  = new CopyOnWriteArrayList<>();
    private final        AtomicBoolean                 started       = new AtomicBoolean(false);
    private final        Map<Digest, SignedJoin>       joins         = new ConcurrentHashMap<>();
    private volatile     Vue                           selected;

    public ViewAssembly(Digest nextViewId, ViewContext vc, Consumer<Assemblies> publisher,
                        CompletableFuture<Vue> onConsensus) {
        view = vc;
        this.nextViewId = nextViewId;
        this.publisher = publisher;
        this.onConsensus = onConsensus;

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

    public void joined(SignedViewMember viewMember) {
        final var mid = Digest.from(viewMember.getVm().getId());
        joins.put(mid, SignedJoin.newBuilder()
                                 .setMember(params().member().getId().toDigeste())
                                 .setJoin(viewMember)
                                 .setSignature(view.sign(viewMember).toSig())
                                 .build());
        if (selected != null && joins.size() >= selected.majority) {
            publishJoins();
        } else if (selected == null) {
            log.trace("Awaiting view selection to publish joins: {} on: {}", joins.size(), params().member().getId());
        } else {
            log.trace("Awaiting required majority: {} of: {} to publish joins: {} on: {}", selected.diadem,
                      selected.majority, joins.size(), params().member().getId());
        }
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        transitions.fsm().enterStartState();
    }

    void assemble(List<Assemblies> asses) {
        if (!started.get()) {
            return;
        }

        if (asses.isEmpty()) {
            return;
        }
        var viewz = asses.stream().flatMap(a -> a.getViewsList().stream()).toList();
        var joinz = asses.stream().flatMap(a -> a.getJoinsList().stream()).toList();
        log.debug("Assemblies: {} joins: {} views: {} on: {}", asses.size(), joinz.size(), viewz.size(),
                  params().member().getId());

        var joins = joinz.stream()
                         .filter(SignedJoin::hasJoin)
                         .filter(view -> !proposals.containsKey(Digest.from(view.getJoin().getVm().getId())))
                         .filter(signedJoin -> !SignedJoin.getDefaultInstance().equals(signedJoin))
                         .filter(view::validate)
                         .toList();
        if (!joins.isEmpty()) {
            log.debug("Assembling joins: {} on: {}", joins.size(), params().member().getId());
            join(joins.stream().map(SignedJoin::getJoin).toList());
        }

        var views = viewz.stream().filter(SignedViews::hasViews).toList();
        if (views.isEmpty()) {
            return;
        }
        log.debug("Assembling views: {} on: {}", views.size(), params().member().getId());

        if (selected != null) {
            log.trace("Already selected: {}, ignoring views: {} on: {}", selected.diadem, views.size(),
                      params().member().getId());
            return;
        }
        views.forEach(svs -> {
            if (view.validate(svs)) {
                log.info("Adding views: {} from: {} on: {}",
                         svs.getViews().getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList(),
                         Digest.from(svs.getViews().getMember()), params().member().getId());
                viewProposals.put(Digest.from(svs.getViews().getMember()), svs.getViews());
            } else {
                log.warn("Invalid views: {} from: {} on: {}",
                         svs.getViews().getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList(),
                         Digest.from(svs.getViews().getMember()), params().member().getId());
            }
        });
        if (viewProposals.size() >= params().majority()) {
            transitions.proposed();
        } else {
            log.trace("Incomplete view proposals: {} is less than majority: {} views: {} on: {}", viewProposals.size(),
                      params().majority(), viewProposals.keySet().stream().sorted().toList(),
                      params().member().getId());
        }
    }

    boolean complete() {
        if (selected == null) {
            log.error("Cannot complete view assembly: {} as selected is null on: {}", nextViewId,
                      params().member().getId());
            transitions.failed();
            return false;
        }
        if (proposals.size() < selected.majority) {
            log.error("Cannot complete view assembly: {} proposed: {} required: {} on: {}", nextViewId,
                      proposals.keySet().stream().sorted().toList(), selected.majority, params().member().getId());
            transitions.failed();
            return false;
        }
        proposals.forEach((d, svm) -> slate.put(d, Join.newBuilder().setMember(svm).build()));

        // Fill out the proposals with the unreachable members of the next assembly
        var missing = Sets.difference(selected.assembly.keySet(), proposals.keySet());
        if (!missing.isEmpty()) {
            log.info("Missing proposals: {} on: {}", missing.stream().sorted().toList(), params().member().getId());
            missing.forEach(m -> slate.put(m, Join.newBuilder()
                                                  .setMember(SignedViewMember.newBuilder()
                                                                             .setVm(ViewMember.newBuilder()
                                                                                              .setId(m.toDigeste())
                                                                                              .setView(
                                                                                              nextViewId.toDigeste())))
                                                  .build()));
        }
        assert slate.size() == selected.assembly.size() : "Invalid slate: " + slate.size() + " expected: "
        + selected.assembly.size();
        log.debug("View Assembly: {} completed assembly: {} on: {}", nextViewId,
                  slate.keySet().stream().sorted().toList(), params().member().getId());
        transitions.complete();
        return true;
    }

    void join(List<SignedViewMember> joins) {
        if (!started.get()) {
            return;
        }
        if (selected == null) {
            pendingJoins.addAll(joins);
            log.trace("Pending joins: {} on: {}", joins.size(), params().member().getId());
            return;
        }
        for (var svm : joins) {
            final var mid = Digest.from(svm.getVm().getId());
            if (proposals.containsKey(mid)) {
                log.trace("Redundant join from: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
                continue;
            }
            if (validate(mid, svm)) {
                proposals.put(mid, svm);
            }
        }
        transitions.checkAssembly();
    }

    void newEpoch() {
        var current = countdown.decrementAndGet();
        if (current == 0) {
            transitions.countdownCompleted();
        }
    }

    private Map<Digest, Member> assemblyOf(List<Digeste> committee) {
        var last = view.pendingViews().last();
        return committee.stream()
                        .map(d -> last.context().getMember(Digest.from(d)))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(Member::getId, m -> m));
    }

    private boolean checkAssembly() {
        if (selected == null) {
            return false;
        }
        if (proposals.size() == selected.assembly.size()) {
            transitions.certified();
            return true;
        }
        return false;
    }

    private Parameters params() {
        return view.params();
    }

    private void propose(Views vs, List<View> majorities, Multiset<View> consensus) {
        var ordered = vs.getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList();
        var lastIndex = -1;
        View last = null;
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

    private void propose() {
        var views = view.pendingViews()
                        .getViews(nextViewId)
                        .setMember(params().member().getId().toDigeste())
                        .setVid(nextViewId.toDigeste())
                        .build();
        log.debug("Proposing for: {} views: {} on: {}", nextViewId,
                  views.getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList(),
                  params().member().getId());
        publisher.accept(Assemblies.newBuilder()
                                   .addViews(
                                   SignedViews.newBuilder().setViews(views).setSignature(view.sign(views).toSig()))
                                   .build());
    }

    private void publishJoins() {
        log.trace("Publish joins: {} on: {}", joins.size(), params().member().getId());
        var b = Assemblies.newBuilder();
        joins.values().forEach(b::addJoins);
        publisher.accept(b.build());
    }

    private boolean validate(Digest mid, SignedViewMember svm) {
        final var m = selected.assembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return false;
        }
        var viewId = Digest.from(svm.getVm().getView());
        if (!nextViewId.equals(viewId)) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view id for member: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("Join of: {} on: {}", print(svm, params().digestAlgorithm()), params().member().getId());
        }

        if (!m.verify(JohnHancock.from(svm.getSignature()), svm.getVm().toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid signature for view member: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return false;
        }

        PubKey encoded = svm.getVm().getConsensusKey();

        if (!m.verify(signature(svm.getVm().getSignature()), encoded.toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Could not verify consensus key from view member: {} on: {}",
                          print(svm, params().digestAlgorithm()), params().member().getId());
            }
            return false;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            if (log.isTraceEnabled()) {
                log.trace("Could not deserialize consensus key from view member: {} on: {}",
                          print(svm, params().digestAlgorithm()), params().member().getId());
            }
            return false;
        }
        log.trace("Validated svm: {} on: {}", print(svm, params().digestAlgorithm()), params().member().getId());
        return true;
    }

    private void vote() {
        Multiset<View> candidates = HashMultiset.create();
        viewProposals.values().forEach(v -> candidates.addAll(v.getViewsList()));
        var majority = params().majority();
        var majorities = candidates.entrySet()
                                   .stream()
                                   .filter(e -> e.getCount() >= majority)
                                   .map(Multiset.Entry::getElement)
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
        Multiset<View> consensus = HashMultiset.create();
        viewProposals.values().forEach(vs -> propose(vs, majorities, consensus));
        var ratification = consensus.entrySet()
                                    .stream()
                                    .filter(e -> e.getCount() >= majority)
                                    .map(Multiset.Entry::getElement)
                                    .sorted(Comparator.comparing(view -> Digest.from(view.getDiadem())))
                                    .toList();
        if (consensus.isEmpty()) {
            log.debug("No consensus views on: {}", params().member().getId());
            return;
        } else if (log.isTraceEnabled()) {
            log.debug("Consensus views: {} on: {}", ratification.stream().map(v -> Digest.from(v.getDiadem())).toList(),
                      params().member().getId());
        }
        var winner = ratification.getFirst();
        selected = new Vue(Digest.from(winner.getDiadem()), assemblyOf(winner.getCommitteeList()),
                           winner.getMajority());
        if (log.isDebugEnabled()) {
            log.debug("Selected: {} on: {}", selected, params().member().getId());
        }
        onConsensus.complete(selected);
        transitions.viewAcquired();
        if (joins.size() >= selected.majority) {
            publishJoins();
        }
        join(pendingJoins);
        pendingJoins.clear();
    }

    public record Vue(Digest diadem, Map<Digest, Member> assembly, int majority) {
        @Override
        public String toString() {
            return "View{" + "diadem=" + diadem + ", assembly=" + assembly.keySet().stream().sorted().toList() + '}';
        }
    }

    private class Recon implements Reconfiguration {
        @Override
        public void certify() {
            if (proposals.size() == selected.assembly.size()) {
                log.info("Certifying: {} majority: {} of: {} slate: {}  on: {}", nextViewId, selected.majority,
                         nextViewId, proposals.keySet().stream().sorted().toList(), params().member().getId());
                transitions.certified();
            } else {
                countdown.set(4);
                log.info("Not certifying: {} majority: {} slate: {} of: {} on: {}", nextViewId, selected.majority,
                         proposals.keySet().stream().sorted().toList(), nextViewId, params().member().getId());
            }
        }

        public void checkAssembly() {
            if (ViewAssembly.this.checkAssembly()) {
                return;
            }
            if (proposals.size() >= selected.majority) {
                transitions.chill();
            } else {
                log.info("Check assembly: {} on: {}", proposals.size(), params().member().getId());
            }
        }

        public void checkViews() {
            vote();
        }

        @Override
        public void chill() {
            countdown.set(-1);
            if (ViewAssembly.this.checkAssembly()) {
                transitions.certified();
            } else {
                countdown.set(2);
            }
        }

        @Override
        public void complete() {
            ViewAssembly.this.complete();
        }

        @Override
        public void failed() {
            view.onFailure();
            log.debug("Failed view assembly for: {} on: {}", nextViewId, params().member().getId());
        }

        @Override
        public void finish() {
            started.set(false);
        }

        @Override
        public void publishViews() {
            propose();
        }

        @Override
        public void vibeCheck() {
            if (ViewAssembly.this.checkAssembly()) {
                transitions.certified();
            }
        }
    }
}
