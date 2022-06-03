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
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;

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
public class ViewAssembly2 implements Reconfiguration {

    record AJoin(Member m, Join j) {}

    private record Proposed(ViewMember vm, Member member, Map<Member, Validate> validations) {}

    private final static Logger log = LoggerFactory.getLogger(ViewAssembly2.class);

    private final SliceIterator<Terminal>     committee;
    private final Map<Digest, Member>         nextAssembly;
    private final Digest                      nextViewId;
    private final Map<Digest, Proposed>       proposals  = new ConcurrentHashMap<>();
    private final Consumer<Reassemble>        publisher;
    private final Map<Member, Join>           slate      = new ConcurrentHashMap<>();
    private final Transitions                 transitions;
    private final Map<Digest, List<Validate>> unassigned = new ConcurrentHashMap<>();
    private final ViewContext                 view;

    public ViewAssembly2(Digest nextViewId, ViewContext vc, Consumer<Reassemble> publisher,
                         CommonCommunications<Terminal, ?> comms) {
        view = vc;
        this.nextViewId = nextViewId;
        this.publisher = publisher;
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

        final Fsm<Reconfiguration, Transitions> fsm = Fsm.construct(this, Transitions.class, Reconfigure.AWAIT_ASSEMBLY,
                                                                    true);
        this.transitions = fsm.getTransitions();
        fsm.setName("View Recon" + params().member().getId());

        log.debug("View Assembly from: {} to: {} recontext: {} next assembly: {} on: {}", view.context().getId(),
                  nextViewId, reContext.getId(), nextAssembly.keySet(), params().member().getId());
    }

    @Override
    public void certify() {
        log.debug("Certifying Join proposals of: {} count: {} state: {} on: {}", nextViewId, proposals.size(),
                  transitions.fsm().getCurrentState(), params().member());
        publisher.accept(Reassemble.newBuilder()
                                   .setValidations(Validations.newBuilder()
                                                              .addAllValidations(proposals.values().stream().map(p -> {
                                                                  return view.generateValidation(p.vm);
                                                              }).toList()))
                                   .build());
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
        AtomicReference<Duration> retryDelay = new AtomicReference<>(Duration.ofMillis(10));
        reiterate.set(() -> committee.iterate((term, m) -> {
            if (proposals.containsKey(m.getId())) {
                return null;
            }
            log.trace("Requesting Join from: {} state: {} on: {}", term.getMember().getId(),
                      transitions.fsm().getCurrentState(), params().member().getId());
            return term.join(request);
        }, (futureSailor, term, m) -> consider(futureSailor, term, m, proceed),
                                              () -> completeSlice(retryDelay, proceed, reiterate)));
        reiterate.get().run();
    }

    public Map<Member, Join> getSlate() {
        final var c = slate;
        return c;
    }

    public Consumer<Reassemble> inbound() {
        return re -> {
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
        };
    }

    @Override
    public void nominate() {
        log.debug("Nominating proposal for: {} members: {} state: {} on: {}", nextViewId,
                  proposals.values().stream().map(p -> p.member.getId()).toList(), transitions.fsm().getCurrentState(),
                  params().member().getId());
        publisher.accept(Reassemble.newBuilder()
                                   .setViewMembers(ViewMembers.newBuilder()
                                                              .addAllMembers(proposals.values()
                                                                                      .stream()
                                                                                      .map(p -> p.vm)
                                                                                      .toList())
                                                              .build())
                                   .build());
    }

    public Parameters params() {
        return view.params();
    }

    public void start() {
        transitions.fsm().enterStartState();
    }

    protected void validate(Validate v) {
        final var cid = Digest.from(v.getWitness().getId());
        var certifier = view.context().getMember(cid);
        if (certifier == null) {
            log.warn("Unknown certifier: {} state: {} on: {}", cid, transitions.fsm().getCurrentState(),
                     params().member());
            return;
        }
        final var hash = Digest.from(v.getHash());
        final var member = nextAssembly.get(hash);
        if (member == null) {
            log.warn("Unknown next view member: {} state: {} on: {}", hash, transitions.fsm().getCurrentState(),
                     params().member());
            return;
        }
        var proposed = proposals.get(hash);
        if (proposed == null) {
            log.warn("Unassigned certification, unknown view join: {} state: {} on: {}", hash,
                     transitions.fsm().getCurrentState(), params().member().getId());
            unassigned.computeIfAbsent(hash, d -> new CopyOnWriteArrayList<>()).add(v);
            return;
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
                               AtomicReference<Runnable> reiterate) {
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
        log.debug("Adding delegate from: {} state: {} on: {}", term.getMember().getId(),
                  transitions.fsm().getCurrentState(), params().member().getId());
        join(member);

        return proceed.get();
    }

    private boolean join(ViewMember vm) {
        final var mid = Digest.from(vm.getId());
        final var m = nextAssembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} state: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                          transitions.fsm().getCurrentState(), params().member().getId());
            }
            return false;
        }

        PubKey encoded = vm.getConsensusKey();

        if (!m.verify(signature(vm.getSignature()), encoded.toByteString())) {
            if (log.isTraceEnabled()) {
                log.trace("Could not verify consensus key from view member: {} state: {} on: {}",
                          ViewContext.print(vm, params().digestAlgorithm()), transitions.fsm().getCurrentState(),
                          params().member().getId());
            }
            return false;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            if (log.isTraceEnabled()) {
                log.trace("Could not deserialize consensus key from view member: {} state: {} on: {}",
                          ViewContext.print(vm, params().digestAlgorithm()), transitions.fsm().getCurrentState(),
                          params().member().getId());
            }
            return false;
        }
        if (log.isTraceEnabled()) {
            log.trace("Valid view member: {} state: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                      transitions.fsm().getCurrentState(), params().member().getId());
        }
        proposals.computeIfAbsent(mid, k -> new Proposed(vm, m, new ConcurrentHashMap<>()));
        return true;
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
}
