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
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.salesforce.apollo.choam.ViewContext.print;
import static com.salesforce.apollo.cryptography.QualifiedBase64.publicKey;
import static com.salesforce.apollo.cryptography.QualifiedBase64.signature;
import static io.grpc.Status.ABORTED;

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
    private final        CompletableFuture<View>       onConsensus;
    private volatile     View                          selected;

    public ViewAssembly(Digest nextViewId, ViewContext vc, Consumer<Assemblies> publisher,
                        CompletableFuture<View> onConsensus) {
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

    public void start() {
        transitions.fsm().enterStartState();
        transitions.assembled();
    }

    void complete() {
        if (selected == null) {
            log.info("Cannot complete view assembly: {} as selected is null on: {}", nextViewId,
                     params().member().getId());
            transitions.failed();
            return;
        }
        if (proposals.size() < selected.majority) {
            log.info("Cannot complete view assembly: {} proposed: {} required: {} on: {}", nextViewId,
                     proposals.keySet().stream().toList(), selected.majority, params().member().getId());
            transitions.failed();
            return;
        }
        // Fill out the proposals with the unreachable members of the next assembly
        Sets.difference(selected.assembly.keySet(), proposals.keySet())
            .forEach(m -> proposals.put(m, SignedViewMember.newBuilder()
                                                           .setVm(ViewMember.newBuilder()
                                                                            .setId(m.toDigeste())
                                                                            .setView(nextViewId.toDigeste()))
                                                           .build()));
        log.debug("View Assembly: {} completed with: {} members on: {}", nextViewId, slate.size(),
                  params().member().getId());
        transitions.complete();
    }

    Consumer<List<Assemblies>> inbound() {
        return lre -> lre.forEach(this::assemble);
    }

    boolean join(SignedViewMember svm, boolean direct) {
        final var mid = Digest.from(svm.getVm().getId());
        final var m = selected.assembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} on: {}", print(svm, params().digestAlgorithm()),
                          params().member().getId());
            }
            throw new StatusRuntimeException(ABORTED);
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
        if (proposals.size() == selected.assembly.size()) {
            Thread.ofVirtual().start(transitions::gathered);
        }
        return true;
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
                log.info("Adding views: {} from: {} on: {}",
                         svs.getViews().getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList(),
                         Digest.from(svs.getViews().getMember()), params().member().getId());
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
                        .collect(Collectors.toMap(Member::getId, m -> m));
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
        Multiset<com.salesforce.apollo.choam.proto.View> consensus = HashMultiset.create();
        viewProposals.values().forEach(vs -> castVote(vs, majorities, consensus));
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
        selected = new View(Digest.from(winner.getDiadem()), assemblyOf(winner.getCommitteeList()),
                            winner.getMajority());
        if (log.isDebugEnabled()) {
            log.debug("Selected: {} on: {}", selected, params().member().getId());
        }
        onConsensus.complete(selected);
        transitions.viewDetermined();
    }

    record View(Digest diadem, Map<Digest, Member> assembly, int majority) {
        @Override
        public String toString() {
            return "View{" + "diadem=" + diadem + ", assembly=" + assembly.keySet().stream().sorted().toList() + '}';
        }
    }

    private class Recon implements Reconfiguration {
        @Override
        public void certify() {
            if (proposals.size() >= selected.majority) {
                log.debug("Certifying: {} majority: {} of: {} slate: {}  on: {}", nextViewId, selected.majority,
                          nextViewId, proposals.keySet().stream().sorted().toList(), params().member().getId());

                proposals.forEach((key, value) -> {
                    slate.put(key, joinOf(value));
                });
                log.debug("Electing view: {} slate: {} on: {}", nextViewId, slate.keySet().stream().sorted().toList(),
                          params().member().getId());
                transitions.certified();
            } else {
                log.debug("Not certifying: {} majority: {} slate: {} of: {} on: {}", nextViewId, selected.majority,
                          proposals.entrySet().stream().sorted().toList(), nextViewId, params().member().getId());
            }
        }

        @Override
        public void complete() {
            ViewAssembly.this.complete();
        }

        @Override
        public void elect() {
            if (selected != null && proposals.size() == selected.assembly().size()) {
                proposals.forEach((key, value) -> slate.put(key, joinOf(value)));
                log.debug("Electing view: {} slate: {} on: {}", nextViewId, slate.keySet().stream().sorted().toList(),
                          params().member().getId());
                transitions.complete();
            } else {
                log.error("Failed election; selected: {} proposed: {} of: {} on: {}", selected != null,
                          proposals.keySet().stream().sorted().toList(), nextViewId, params().member().getId());
                transitions.failed();
            }
        }

        @Override
        public void failed() {
            view.onFailure();
            log.debug("Failed view assembly for: {} on: {}", nextViewId, params().member().getId());
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
