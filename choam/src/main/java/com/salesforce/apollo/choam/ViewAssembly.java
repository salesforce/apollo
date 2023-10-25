/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.chiralbehaviors.tron.Fsm;
import com.salesfoce.apollo.choam.proto.*;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Reconfiguration;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Reconfigure;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Transitions;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.ring.SliceIterator;
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

import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

/**
 * View reconfiguration. Attempts to create a new view reconfiguration. View reconfiguration needs at least 2f+1
 * certified members from the next view. The protol finishes with a list of at least 2f+1 Joins with at least 2f+1
 * certifications from the current view, or fails
 *
 * @author hal.hildebrand
 */
public class ViewAssembly {

    private final static Logger                      log         = LoggerFactory.getLogger(ViewAssembly.class);
    protected final      Transitions                 transitions;
    private final        AtomicBoolean               cancelSlice = new AtomicBoolean();
    private final        SliceIterator<Terminal>     committee;
    private final        Map<Digest, Member>         nextAssembly;
    private final        Digest                      nextViewId;
    private final        Map<Digest, Proposed>       proposals   = new ConcurrentHashMap<>();
    private final        Consumer<Reassemble>        publisher;
    private final        Map<Member, Join>           slate       = new ConcurrentHashMap<>();
    private final        Map<Digest, List<Validate>> unassigned  = new ConcurrentHashMap<>();
    private final        ViewContext                 view;

    public ViewAssembly(Digest nextViewId, ViewContext vc, Consumer<Reassemble> publisher,
                        CommonCommunications<Terminal, ?> comms, Executor executor) {
        view = vc;
        this.nextViewId = nextViewId;
        this.publisher = publisher;
        nextAssembly = Committee.viewMembersOf(nextViewId, params().context())
                                .stream()
                                .collect(Collectors.toMap(m -> m.getId(), m -> m));
        var slice = new ArrayList<>(nextAssembly.values());
        committee = new SliceIterator<Terminal>("Committee for " + nextViewId, params().member(), slice, comms,
                                                executor);

        final Fsm<Reconfiguration, Transitions> fsm = Fsm.construct(new Recon(), Transitions.class,
                                                                    Reconfigure.AWAIT_ASSEMBLY, true);
        this.transitions = fsm.getTransitions();
        fsm.setName("View Recon" + params().member().getId());

        log.debug("View reconfiguration from: {} to: {}, next assembly: {} on: {}", view.context().getId(), nextViewId,
                  nextAssembly.keySet(), params().member().getId());
    }

    public Map<Member, Join> getSlate() {
        final var c = slate;
        return c;
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
        if (slate.size() < params().context().majority()) {
            proposals.values()
                     .stream()
                     .filter(p -> p.validations.size() >= view.context().majority())
                     .sorted(Comparator.comparing(p -> p.member.getId()))
                     .forEach(p -> slate.put(p.member(), joinOf(p)));
            if (slate.size() >= params().context().majority()) {
                log.debug("Complete.  Electing slate: {} of: {} on: {}", slate.size(), nextViewId, params().member());
            } else {
                log.error("Failed completion, election required: {} slate: {} of: {} on: {}",
                          params().context().majority() + 1, proposals.values()
                                                                      .stream()
                                                                      .map(p -> String.format("%s:%s", p.member.getId(),
                                                                                              p.validations.size()))
                                                                      .toList(), nextViewId, params().member());
                transitions.failed();
            }
        }
        log.debug("View Assembly: {} completed with: {} members on: {}", nextViewId, slate.size(),
                  params().member().getId());
    }

    void finalElection() {
        transitions.complete();
    }

    Consumer<List<Reassemble>> inbound() {
        return lre -> {
            lre.stream()
               .flatMap(re -> re.getMembersList().stream())
               .map(e -> join(e))
               .filter(r -> r != null)
               .reduce((a, b) -> Reassemble.newBuilder(a)
                                           .addAllMembers(b.getMembersList())
                                           .addAllValidations(b.getValidationsList())
                                           .build())
               .ifPresent(publisher);
            lre.stream().flatMap(re -> re.getValidationsList().stream()).forEach(e -> validate(e));
        };
    }

    private void completeSlice(AtomicReference<Duration> retryDelay, AtomicReference<Runnable> reiterate) {
        if (gathered()) {
            return;
        }

        final var delay = retryDelay.get();
        if (delay.compareTo(params().producer().maxGossipDelay()) < 0) {
            retryDelay.accumulateAndGet(Duration.ofMillis(100), (a, b) -> a.plus(b));
        }

        log.trace("Proposal incomplete of: {} gathered: {} desired: {}, retrying: {} on: {}", nextViewId,
                  proposals.keySet().stream().toList(), nextAssembly.size(), delay, params().member().getId());
        if (!cancelSlice.get()) {
            Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory())
                     .schedule(() -> reiterate.get().run(), delay.toMillis(), TimeUnit.MILLISECONDS);
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
        var reassemble = join(member);
        if (reassemble != null) {
            publisher.accept(reassemble);
        }
        return !gathered();
    }

    private boolean gathered() {
        boolean complete = proposals.size() == nextAssembly.size();
        if (complete) {
            cancelSlice.set(true);
        }
        return complete;
    }

    private Reassemble join(ViewMember vm) {
        final var mid = Digest.from(vm.getId());
        final var m = nextAssembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return null;
        }

        PubKey encoded = vm.getConsensusKey();

        if (!m.verify(signature(vm.getSignature()), encoded.toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Could not verify consensus key from view member: {} on: {}",
                          ViewContext.print(vm, params().digestAlgorithm()), params().member().getId());
            }
            return null;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            if (log.isTraceEnabled()) {
                log.trace("Could not deserialize consensus key from view member: {} on: {}",
                          ViewContext.print(vm, params().digestAlgorithm()), params().member().getId());
            }
            return null;
        }
        AtomicBoolean newJoin = new AtomicBoolean();

        var proposed = proposals.computeIfAbsent(mid, k -> {
            newJoin.set(true);
            return new Proposed(vm, m, new ConcurrentHashMap<>());
        });
        proposed.validations.computeIfAbsent(params().member(), k -> view.generateValidation(vm));

        if (newJoin.get()) {
            if (log.isTraceEnabled()) {
                log.trace("Adding view member: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                          params().member().getId());
            }
            var validations = unassigned.remove(mid);
            if (validations != null) {
                validations.forEach(v -> validate(v));
            }
            if (proposals.size() == nextAssembly.size()) {
                transitions.gathered();
            }
            return Reassemble.newBuilder()
                             .addMembers(vm)
                             .addValidations(proposed.validations.get(params().member()))
                             .build();
        }
        return null;
    }

    private Join joinOf(Proposed candidate) {
        final List<Certification> witnesses = candidate.validations.values()
                                                                   .stream()
                                                                   .map(v -> v.getWitness())
                                                                   .sorted(
                                                                   Comparator.comparing(c -> new Digest(c.getId())))
                                                                   .collect(Collectors.toList());
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
            log.warn("Unknown certifier: {} on: {}", cid, params().member());
            return;
        }
        final var digest = Digest.from(v.getHash());
        final var member = nextAssembly.get(digest);
        if (member == null) {
            log.warn("Unknown next view member: {} on: {}", digest, params().member());
            return;
        }
        var proposed = proposals.get(digest);
        if (proposed == null) {
            log.warn("Unassigned certification, unknown view join: {} on: {}", digest, params().member().getId());
            unassigned.computeIfAbsent(digest, d -> new CopyOnWriteArrayList<>()).add(v);
            return;
        }
        if (!view.validate(proposed.vm, v)) {
            log.warn("Invalid cetification for view join: {} from: {} on: {}", digest,
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

    record AJoin(Member m, Join j) {
    }

    private record Proposed(ViewMember vm, Member member, Map<Member, Validate> validations) {
    }

    private class Recon implements Reconfiguration {

        @Override
        public void certify() {
            if (proposals.values().stream().filter(p -> p.validations.size() == nextAssembly.size()).count()
            == nextAssembly.size()) {
                cancelSlice.set(true);
                log.debug("Certifying slate: {} of: {} on: {}", proposals.size(), nextViewId, params().member());
                transitions.certified();
            }
            log.debug("Not certifying slate: {} of: {} on: {}", proposals.entrySet()
                                                                         .stream()
                                                                         .map(e -> String.format("%s:%s", e.getKey(),
                                                                                                 e.getValue().validations.size()))
                                                                         .toList(), nextViewId, params().member());
        }

        @Override
        public void complete() {
            ViewAssembly.this.complete();
        }

        @Override
        public void elect() {
            proposals.values()
                     .stream()
                     .filter(p -> p.validations.size() >= view.context().majority())
                     .sorted(Comparator.comparing(p -> p.member.getId()))
                     .forEach(p -> slate.put(p.member(), joinOf(p)));
            if (slate.size() >= params().context().majority()) {
                cancelSlice.set(true);
                log.debug("Electing slate: {} of: {} on: {}", slate.size(), nextViewId, params().member());
                transitions.complete();
            } else {
                log.error("Failed election, required: {} slate: {} of: {} on: {}", params().context().majority() + 1,
                          proposals.values()
                                   .stream()
                                   .map(p -> String.format("%s:%s", p.member.getId(), p.validations.size()))
                                   .toList(), nextViewId, params().member());
            }
        }

        @Override
        public void failed() {
            stop();
            log.error("Failed view assembly for: {} on: {}", nextViewId, params().member());
        }

        @Override
        public void gather() {
            log.trace("Gathering assembly for: {} on: {}", nextViewId, params().member());
            AtomicReference<Runnable> reiterate = new AtomicReference<>();
            AtomicReference<Duration> retryDelay = new AtomicReference<>(Duration.ofMillis(10));
            reiterate.set(() -> committee.iterate((term, m) -> {
                                                      if (proposals.containsKey(m.getId())) {
                                                          return null;
                                                      }
                                                      log.trace("Requesting Join from: {} on: {}", term.getMember().getId(), params().member().getId());
                                                      return term.join(nextViewId);
                                                  }, (futureSailor, term, m) -> consider(futureSailor, term, m), () -> completeSlice(retryDelay, reiterate),
                                                  Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()),
                                                  params().gossipDuration()));
            reiterate.get().run();
        }

        @Override
        public void nominate() {
            publisher.accept(Reassemble.newBuilder()
                                       .addAllMembers(proposals.values().stream().map(p -> p.vm).toList())
                                       .addAllValidations(proposals.values()
                                                                   .stream()
                                                                   .flatMap(p -> p.validations.values().stream())
                                                                   .toList())
                                       .build());
            transitions.nominated();
        }
    }
}
