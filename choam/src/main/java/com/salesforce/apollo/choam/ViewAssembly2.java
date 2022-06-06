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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reassemble2;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Reconfiguration2;
import com.salesforce.apollo.choam.fsm.Reconfiguration2.Reconfigure;
import com.salesforce.apollo.choam.fsm.Reconfiguration2.Transitions;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.Entropy;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * View reconfiguration. Attempts to create a new view reconfiguration. View
 * reconfiguration needs at least 2f+1 certified members from the next view. The
 * protol finishes with a list of at least 2f+1 Joins with at least 2f+1
 * certifications from the current view, or fails
 * 
 * @author hal.hildebrand
 *
 */
public class ViewAssembly2 {

    record AJoin(Member m, Join j) {}

    private class Recon implements Reconfiguration2 {

        @Override
        public void certify() {
            var certify = proposals.values()
                                   .stream()
                                   .filter(p -> p.validations.size() >= params().majority())
                                   .sorted(Comparator.comparing(p -> p.member.getId()))
                                   .collect(Collectors.toMap(p -> p.member, p -> joinOf(p)));
            if (certify.size() >= params().majority()) {
                log.debug("Certifying slate: {} of: {} on: {}", certify.size(), nextViewId, params().member());
                transitions.certified();
            }
        }

        @Override
        public void complete() {
            ViewAssembly2.this.complete();
        }

        @Override
        public void elect() {
            proposals.values()
                     .stream()
                     .filter(p -> p.validations.size() >= params().majority())
                     .sorted(Comparator.comparing(p -> p.member.getId()))
                     .forEach(p -> slate.put(p.member(), joinOf(p)));
            if (slate.size() >= params().majority()) {
                log.debug("Electing slate: {} of: {} on: {}", slate.size(), nextViewId, params().member());
                transitions.complete();
            } else {
                transitions.failed();
            }
        }

        @Override
        public void failed() {
            log.error("Failed view assembly for: {} on: {}", nextViewId, params().member());
        }

        @Override
        public void gather() {
            log.trace("Gathering assembly for: {} on: {}", nextViewId, params().member());
            JoinRequest request = JoinRequest.newBuilder()
                                             .setContext(params().context().getId().toDigeste())
                                             .setNextView(nextViewId.toDigeste())
                                             .build();
            AtomicReference<Runnable> reiterate = new AtomicReference<>();
            AtomicReference<Duration> retryDelay = new AtomicReference<>(Duration.ofMillis(10));
            reiterate.set(() -> committee.iterate((term, m) -> {
                if (proposals.containsKey(m.getId())) {
                    return null;
                }
                log.trace("Requesting Join from: {} on: {}", term.getMember().getId(), params().member().getId());
                return term.join(request);
            }, (futureSailor, term, m) -> consider(futureSailor, term, m), () -> completeSlice(retryDelay, reiterate)));
            reiterate.get().run();
        }

        @Override
        public void nominate() {
            publisher.accept(Reassemble2.newBuilder()
                                        .addAllMembers(proposals.values().stream().map(p -> p.vm).toList())
                                        .addAllValidations(proposals.values()
                                                                    .stream()
                                                                    .flatMap(p -> p.validations.values().stream())
                                                                    .toList())
                                        .build());
            transitions.nominated();
        }
    }

    private record Proposed(ViewMember vm, Member member, Map<Member, Validate> validations) {}

    private final static Logger log = LoggerFactory.getLogger(ViewAssembly2.class);

    protected final Transitions               transitions;
    private final AtomicBoolean               cancelSlice = new AtomicBoolean();
    private final SliceIterator<Terminal>     committee;
    private final Map<Digest, Member>         nextAssembly;
    private final Digest                      nextViewId;
    private final Map<Digest, Proposed>       proposals   = new ConcurrentHashMap<>();
    private final Consumer<Reassemble2>       publisher;
    private final Map<Member, Join>           slate       = new ConcurrentHashMap<>();
    private final Map<Digest, List<Validate>> unassigned  = new ConcurrentHashMap<>();
    private final ViewContext                 view;

    public ViewAssembly2(Digest nextViewId, ViewContext vc, Consumer<Reassemble2> publisher,
                         CommonCommunications<Terminal, ?> comms) {
        view = vc;
        this.nextViewId = nextViewId;
        this.publisher = publisher;
        nextAssembly = Committee.viewMembersOf(nextViewId, params().context())
                                .stream()
                                .collect(Collectors.toMap(m -> m.getId(), m -> m));
        var slice = new ArrayList<>(nextAssembly.values());
        Entropy.secureShuffle(slice);
        committee = new SliceIterator<Terminal>("Committee for " + nextViewId, params().member(), slice, comms,
                                                params().exec());

        final Fsm<Reconfiguration2, Transitions> fsm = Fsm.construct(new Recon(), Transitions.class,
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

    void assembled() {
        transitions.assembled();
    }

    void complete() {
        log.debug("View Assembly: {} completed with: {} members on: {}", nextViewId, slate.size(),
                  params().member().getId());
    }

    Consumer<Reassemble2> inbound() {
        return re -> {
            Optional<Reassemble2> reassemble = re.getMembersList()
                                                 .stream()
                                                 .map(e -> join(e))
                                                 .filter(r -> r != null)
                                                 .reduce((a, b) -> Reassemble2.newBuilder(a)
                                                                              .addAllMembers(b.getMembersList())
                                                                              .addAllValidations(b.getValidationsList())
                                                                              .build());
            re.getValidationsList().stream().forEach(e -> validate(e));
            reassemble.ifPresent(publisher);
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

        log.trace("Proposal incomplete of: {} gathered: {} required: {}, retrying: {} on: {}", nextViewId,
                  proposals.keySet().stream().toList(), nextAssembly.size(), delay, params().member().getId());
        if (!cancelSlice.get()) {
            params().scheduler().schedule(() -> reiterate.get().run(), delay.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private boolean consider(Optional<ListenableFuture<ViewMember>> futureSailor, Terminal term, Member m) {
        if (futureSailor.isEmpty()) {
            return !gathered();
        }
        ViewMember member;
        try {
            member = futureSailor.get().get();
            log.debug("Join reply from: {} on: {}", term.getMember().getId(), params().member().getId());
        } catch (InterruptedException e) {
            log.debug("Error join response from: {} on: {}", term.getMember().getId(), params().member().getId(), e);
            return true;
        } catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                if (!sre.getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
                    log.debug("Error join response from: {} on: {}", term.getMember().getId(),
                              params().member().getId(), sre);
                }
            } else {
                log.trace("Error join response from: {} on: {}", term.getMember().getId(), params().member().getId(),
                          e.getCause());
            }
            return !gathered();
        }
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
            if (cancelSlice.compareAndSet(false, true)) {
                log.trace("Proposal assembled: {} on: {}", nextViewId, params().member().getId());
                transitions.gathered();
            }
        } else {
            log.trace("Proposal incomplete: {} have: {} want: {} on: {}", nextViewId, proposals.size(),
                      nextAssembly.size(), params().member().getId());
        }
        return complete;
    }

    private Reassemble2 join(ViewMember vm) {
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
            return Reassemble2.newBuilder()
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
                                                                   .sorted(Comparator.comparing(c -> new Digest(c.getId())))
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
        proposed.validations.computeIfAbsent(certifier, k -> {
            log.debug("Validation of view member: {}:{} using certifier: {} on: {}", member.getId(), digest,
                      certifier.getId(), params().member().getId());
            return v;
        });
    }
}
