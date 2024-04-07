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
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.proto.PubKey;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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
    private final        Map<Digest, Proposed>             proposals   = new ConcurrentHashMap<>();
    private final        Consumer<Reassemble>              publisher;
    private final        Map<Member, Join>                 slate       = new ConcurrentSkipListMap<>();
    private final        Map<Digest, List<Validate>>       unassigned  = new ConcurrentHashMap<>();
    private final        ViewContext                       view;
    private final        CommonCommunications<Terminal, ?> comms;
    private final        Set<Digest>                       polled      = Collections.newSetFromMap(
    new ConcurrentSkipListMap<>());
    private volatile     Map<Digest, Member>               nextAssembly;

    public ViewAssembly(Digest nextViewId, ViewContext vc, Consumer<Reassemble> publisher,
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

    public Map<Member, Join> getSlate() {
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
        proposals.values()
                 .stream()
                 .filter(p -> p.validations.size() >= params().majority())
                 .forEach(p -> slate.put(p.member(), joinOf(p)));
        Context<Member> pendingContext = view.pendingView();
        if (slate.size() >= pendingContext.majority()) {
            log.debug("View Assembly: {} completed with: {} members on: {}", nextViewId, slate.size(),
                      params().member().getId());
        } else {
            log.debug("Failed view assembly completion, election required: {} slate: {} of: {} on: {}",
                      pendingContext.majority(), proposals.values()
                                                          .stream()
                                                          .map(p -> String.format("%s:%s", p.member.getId(),
                                                                                  p.validations.size()))
                                                          .sorted()
                                                          .toList(), nextViewId, params().member().getId());
            transitions.failed();
        }
    }

    void finalElection() {
        transitions.complete();
    }

    Consumer<List<Reassemble>> inbound() {
        return lre -> {
            lre.stream().flatMap(re -> re.getMembersList().stream()).forEach(vm -> join(vm, false));
            lre.stream().flatMap(re -> re.getValidationsList().stream()).forEach(this::validate);
        };
    }

    private void completeSlice(AtomicReference<Duration> retryDelay, AtomicReference<Runnable> reiterate) {
        if (gathered()) {
            return;
        }

        final var delay = retryDelay.get();
        if (delay.compareTo(params().producer().maxGossipDelay()) < 0) {
            retryDelay.accumulateAndGet(Duration.ofMillis(100), Duration::plus);
        }

        log.trace("Proposal incomplete of: {} gathered: {}, required: {} available: {}, retrying: {} on: {}",
                  nextViewId, proposals.keySet().stream().toList(), nextAssembly.size(), params().majority(), delay,
                  params().member().getId());
        if (!cancelSlice.get()) {
            Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory())
                     .schedule(() -> Thread.ofVirtual().start(Utils.wrapped(reiterate.get(), log)), delay.toMillis(),
                               TimeUnit.MILLISECONDS);
        }
    }

    private boolean consider(Optional<ViewMember> futureSailor, Terminal term, Member m) {
        if (futureSailor.isEmpty()) {
            return !gathered();
        }
        ViewMember member;
        member = futureSailor.get();
        log.debug("Join reply from: {} on: {}", term.getMember().getId(), params().member().getId());
        if (member.equals(ViewMember.getDefaultInstance())) {
            log.debug("Empty join response from: {} on: {}", term.getMember().getId(), params().member().getId());
            return !gathered();
        }
        var vm = new Digest(member.getId());
        if (!m.getId().equals(vm)) {
            log.debug("Invalid join response from: {} expected: {} on: {}", term.getMember().getId(), vm,
                      params().member().getId());
            return !gathered();
        }
        join(member, true);
        return !gathered();
    }

    private boolean gathered() {
        if (polled.size() == nextAssembly.size()) {
            log.info("Polled all +");
            cancelSlice.set(true);
            return true;
        }
        return false;
    }

    private Reassemble getMemberProposal() {
        return Reassemble.newBuilder()
                         .addAllMembers(proposals.values().stream().map(p -> p.vm).toList())
                         .addAllValidations(
                         proposals.values().stream().flatMap(p -> p.validations.values().stream()).toList())
                         .build();
    }

    private void join(ViewMember vm, boolean direct) {
        final var mid = Digest.from(vm.getId());
        final var m = nextAssembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Join request from: {} vm: {} on: {}", m.getId(),
                      ViewContext.print(vm, params().digestAlgorithm()), params().member().getId());
        }
        PubKey encoded = vm.getConsensusKey();

        if (!m.verify(signature(vm.getSignature()), encoded.toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Could not verify consensus key from view member: {} on: {}",
                          ViewContext.print(vm, params().digestAlgorithm()), params().member().getId());
            }
            return;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            if (log.isTraceEnabled()) {
                log.trace("Could not deserialize consensus key from view member: {} on: {}",
                          ViewContext.print(vm, params().digestAlgorithm()), params().member().getId());
            }
            return;
        }
        AtomicBoolean newJoin = new AtomicBoolean();

        var proposed = proposals.computeIfAbsent(mid, k -> {
            newJoin.set(true);
            return new Proposed(vm, m, new ConcurrentSkipListMap<>());
        });

        var builder = Reassemble.newBuilder();
        proposed.validations.computeIfAbsent(params().member(), k -> {
            var validate = view.generateValidation(vm);
            builder.addValidations(validate);
            return validate;
        });

        if (newJoin.get()) {
            if (log.isTraceEnabled()) {
                log.trace("Adding view member: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                          params().member().getId());
            }
            if (direct) {
                builder.addMembers(vm);
            }
            var validations = unassigned.remove(mid);
            if (validations != null) {
                validations.forEach(this::validate);
            }
        }
        polled.add(mid);
        var reass = builder.build();
        if (reass.isInitialized()) {
            publisher.accept(reass);
        }
    }

    private Join joinOf(Proposed candidate) {
        final List<Certification> witnesses = candidate.validations.values()
                                                                   .stream()
                                                                   .map(Validate::getWitness)
                                                                   .sorted(
                                                                   Comparator.comparing(c -> new Digest(c.getId())))
                                                                   .toList();
        return Join.newBuilder()
                   .setMember(candidate.vm)
                   .setView(nextViewId.toDigeste())
                   .addAllEndorsements(witnesses)
                   .build();
    }

    private Parameters params() {
        return view.params();
    }

    private void validate(Validate v) {
        final var cid = Digest.from(v.getWitness().getId());
        var certifier = view.context().getMember(cid);
        if (certifier == null) {
            log.warn("Unknown certifier: {} on: {}", cid, params().member().getId());
            return;
        }
        final var digest = Digest.from(v.getHash());
        final var member = nextAssembly.get(digest);
        if (member == null) {
            log.warn("Unknown next view member: {} on: {}", digest, params().member().getId());
            return;
        }
        var proposed = proposals.get(digest);
        if (proposed == null) {
            log.warn("Unassigned certification, unknown view join: {} on: {}", digest, params().member().getId());
            unassigned.computeIfAbsent(digest, d -> new CopyOnWriteArrayList<>()).add(v);
            return;
        }
        if (!view.validate(proposed.vm, v)) {
            log.warn("Invalid certification for view join: {} from: {} on: {}", digest,
                     Digest.from(v.getWitness().getId()), params().member().getId());
            return;
        }
        var newCertifier = new AtomicBoolean();
        proposed.validations.computeIfAbsent(certifier, k -> {
            log.debug("Validation of view member: {}:{} using certifier: {} on: {}", member.getId(), digest,
                      certifier.getId(), params().member().getId());
            newCertifier.set(true);
            return v;
        });
        if (newCertifier.get()) {
            transitions.validation();
        }
    }

    private record Proposed(ViewMember vm, Member member, Map<Member, Validate> validations) {
    }

    private class Recon implements Reconfiguration {

        @Override
        public void certify() {
            var certified = proposals.entrySet()
                                     .stream()
                                     .filter(p -> p.getValue().validations.size() >= params().majority())
                                     .map(Map.Entry::getKey)
                                     .sorted()
                                     .toList();
            Context<Member> memberContext = view.pendingView();
            var required = memberContext.majority();
            if (certified.size() >= required) {
                cancelSlice.set(true);
                log.debug("Certifying: {} required: {} of: {} slate: {}  on: {}", certified.size(), required,
                          nextViewId, certified, params().member().getId());
                transitions.certified();
            } else {
                log.debug("Not certifying: {} required: {} slate: {} of: {} on: {}", certified.size(), required,
                          proposals.entrySet()
                                   .stream()
                                   .map(e -> String.format("%s:%s", e.getKey(), e.getValue().validations.size()))
                                   .sorted()
                                   .toList(), nextViewId, params().member().getId());
            }
        }

        @Override
        public void complete() {
            ViewAssembly.this.complete();
        }

        @Override
        public void elect() {
            proposals.values()
                     .stream()
                     .filter(p -> p.validations.size() >= params().majority())
                     .sorted(Comparator.comparing(p -> p.member.getId()))
                     .forEach(p -> slate.put(p.member(), joinOf(p)));
            if (slate.size() >= view.pendingView().majority()) {
                cancelSlice.set(true);
                log.debug("Electing: {} of: {} slate: {} proposals: {} on: {}", slate.size(), nextViewId,
                          slate.keySet().stream().map(Member::getId).sorted().toList(), proposals.values()
                                                                                                 .stream()
                                                                                                 .map(
                                                                                                 p -> String.format(
                                                                                                 "%s:%s",
                                                                                                 p.member.getId(),
                                                                                                 p.validations.size()))
                                                                                                 .sorted()
                                                                                                 .toList(),
                          params().member().getId());
                transitions.complete();
            } else {
                Context<Member> memberContext = view.pendingView();
                log.error("Failed election, required: {} slate: {} of: {} on: {}", memberContext.majority(),
                          proposals.values()
                                   .stream()
                                   .map(p -> String.format("%s:%s", p.member.getId(), p.validations.size()))
                                   .sorted()
                                   .toList(), nextViewId, params().member().getId());
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
            AtomicReference<Duration> retryDelay = new AtomicReference<>(Duration.ofMillis(10));
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
            publisher.accept(getMemberProposal());
            transitions.nominated();
        }
    }
}
