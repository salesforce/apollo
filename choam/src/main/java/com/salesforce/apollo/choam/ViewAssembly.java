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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
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
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.memberships.ContextGossiper;
import com.salesforce.apollo.membership.Context;
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

    static class OneShot implements Supplier<ByteString> {
        private CountDownLatch      latch = new CountDownLatch(1);
        private volatile ByteString value;

        @Override
        public ByteString get() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                return ByteString.EMPTY;
            }
            final var current = value;
            value = null;
            return current == null ? ByteString.EMPTY : current;
        }

        void setValue(ByteString value) {
            this.value = value;
            latch.countDown();
        }
    }

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
    private final Controller              controller;
    private final ContextGossiper         coordinator;
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
                                                new ArrayList<>(nextAssembly.values()), comms);
        // Create a new context for reconfiguration
        final Digest reconPrefixed = view.context().getId().xor(nextViewId);
        Context<Member> reContext = new Context<Member>(reconPrefixed, 0.33, view.context().memberCount(), 3);
        view.context().allMembers().forEach(e -> reContext.add(e));

        final Fsm<Reconfiguration, Transitions> fsm = Fsm.construct(this, Transitions.class, getStartState(), true);
        this.transitions = fsm.getTransitions();
        fsm.setName("View Recon" + params().member().getId());

        Config.Builder config = params().producer().ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        Short pid = view.roster().get(params().member().getId());
        if (pid == null) {
            config.setPid((short) 0).setnProc((short) 1);
        } else {
            config.setPid(pid).setnProc((short) view.roster().size());
        }
        config.setEpochLength(7).setNumberOfEpochs(epochs());
        controller = new Ethereal().deterministic(config.build(), dataSource(),
                                                  (preblock, last) -> process(preblock, last),
                                                  epoch -> transitions.nextEpoch(epoch));

        coordinator = new ContextGossiper(controller, reContext, params().member(), params().communications(),
                                          params().exec(), params().metrics());

        log.debug("View Assembly from: {} to: {} recontext: {} next assembly: {} on: {}", view.context().getId(),
                  nextViewId, reContext.getId(), nextAssembly.keySet(), params().member());
    }

    @Override
    public void certify() {
        ds = new OneShot();
        log.debug("Certifying Join proposals of: {} count: {} on: {}", nextViewId, proposals.size(), params().member());
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
        log.debug("View Assembly: {} completed with: {} members on: {}", nextViewId, slate.size(), params().member());
    }

    @Override
    public void elect() {
        proposals.values()
                 .stream()
                 .filter(p -> p.validations.size() > params().toleranceLevel())
                 .sorted(Comparator.comparing(p -> p.member.getId()))
                 .forEach(p -> slate.put(p.member(), joinOf(p)));
        if (slate.size() > params().toleranceLevel()) {
            log.debug("Electing slate: {} of: {} on: {}", slate.size(), nextViewId, params().member());
            transitions.complete();
        } else {
            log.debug("Failed to elect slate: {} of: {} on: {}", slate.size(), nextViewId, params().member());
            transitions.failed();
        }
    }

    @Override
    public void failed() {
        stop();
    }

    @Override
    public void gather() {
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
            log.trace("Requesting Join from: {} on: {}", term.getMember().getId(), params().member());
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
        log.debug("Nominating proposal for: {} members: {} on: {}", nextViewId,
                  proposals.values().stream().map(p -> p.member.getId()).toList(), params().member());
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
        log.trace("Stopping view assembly: {} on: {}", nextViewId, params().member());
        coordinator.stop();
        controller.stop();
        final var cur = blockingThread;
        blockingThread = null;
        if (cur != null) {
            cur.interrupt();
        }
    }

    protected int epochs() {
        return 3;
    }

    protected Reconfigure getStartState() {
        return Reconfigure.AWAIT_ASSEMBLY;
    }

    protected void validate(Validate v) {
        final var cid = Digest.from(v.getWitness().getId());
        var certifier = view.context().getMember(cid);
        if (certifier == null) {
            log.warn("Unknown certifier: {} on: {}", cid, params().member());
            return; // do not have the join yet
        }
        final var hash = Digest.from(v.getHash());
        final var member = nextAssembly.get(hash);
        if (member == null) {
            return;
        }
        var proposed = proposals.get(hash);
        if (proposed == null) {
            log.warn("Invalid certification, unknown view join: {} on: {}", hash, params().member());
            return; // do not have the join yet
        }
        if (!view.validate(proposed.vm, v)) {
            log.warn("Invalid cetification for view join: {} from: {} on: {}", hash,
                     Digest.from(v.getWitness().getId()), params().member());
            return;
        }
        proposed.validations.put(certifier, v);
        log.debug("Validation of view member: {}:{} using certifier: {} on: {}", member.getId(), hash,
                  certifier.getId(), params().member());
    }

    void assembled() {
        transitions.assembled();
    }

    private void completeSlice(AtomicReference<Duration> retryDelay, AtomicBoolean proceed,
                               AtomicReference<Runnable> reiterate, AtomicInteger countDown) {
        final var count = proposals.size();
        if (count == nextAssembly.size()) {
            proceed.set(false);
            log.trace("Proposal assembled: {} on: {}", nextViewId, params().member());
            transitions.gathered();
            return;
        }

        final var delay = retryDelay.get();
        if (delay.compareTo(params().producer().maxGossipDelay()) < 0) {
            retryDelay.accumulateAndGet(Duration.ofMillis(100), (a, b) -> a.plus(b));
        }

        if (count > params().toleranceLevel()) {
            if (countDown.decrementAndGet() >= 0) {
                log.trace("Retrying, attempting full assembly of: {} gathered: {} desired: {} on: {}", nextViewId,
                          proposals.keySet().stream().toList(), nextAssembly.size(), params().member());
                proceed.set(true);
                params().scheduler().schedule(() -> reiterate.get().run(), delay.toMillis(), TimeUnit.MILLISECONDS);
                return;
            }
            proceed.set(false);
            log.trace("Proposal assembled: {} gathered: {} out of: {} on: {}", nextViewId, count, nextAssembly.size(),
                      params().member());
            transitions.gathered();
            return;
        }

        log.trace("Proposal incomplete of: {} gathered: {} required: {}, retrying on: {}", nextViewId,
                  proposals.keySet().stream().toList(), params().toleranceLevel() + 1, params().member());
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
            log.debug("Join reply from: {} on: {}", term.getMember().getId(), params().member().getId());
        } catch (InterruptedException e) {
            log.debug("Error join response from: {} on: {}", term.getMember().getId(), params().member().getId(), e);
            return proceed.get();
        } catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                if (!sre.getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
                    log.debug("Error join response from: {} on: {}", term.getMember().getId(),
                              params().member().getId(), sre);
                }
            }
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
        log.debug("Adding delegate to: {} from: {} on: {}", getViewId(), term.getMember().getId(), params().member());
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
                    final var take = ds.get();
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
            log.trace("Invalid view member: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                      params().member());
            return;
        }

        PubKey encoded = vm.getConsensusKey();

        if (!m.verify(signature(vm.getSignature()), encoded.toByteString())) {
            log.trace("Could not verify consensus key from view member: {} on: {}",
                      ViewContext.print(vm, params().digestAlgorithm()), params().member());
            return;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            log.trace("Could not deserialize consensus key from view member: {} on: {}",
                      ViewContext.print(vm, params().digestAlgorithm()), params().member());
            return;
        }
        log.trace("Valid view member: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()), params().member());
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
        preblock.data().stream().map(e -> {
            try {
                return Reassemble.parseFrom(e);
            } catch (InvalidProtocolBufferException ex) {
                log.trace("Error parsing Reassemble on: {}", params().member());
                return (Reassemble) null;
            }
        }).filter(e -> e != null).forEach(re -> process(re));
        if (last) {
            transitions.complete();
        }
    }

    private void process(Reassemble re) {
        if (re.getAssemblyCase() != AssemblyCase.ASSEMBLY_NOT_SET) {
            log.trace("Processing: {} on: {}", re.getAssemblyCase(), params().member());
        }
        switch (re.getAssemblyCase()) {
        case VALIDATIONS:
            re.getValidations().getValidationsList().stream().forEach(e -> validate(e));
            break;
        case VIEWMEMBERS:
            re.getViewMembers().getMembersList().forEach(e -> join(e));
            break;
        default:
            break;
        }
    }
}
