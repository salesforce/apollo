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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
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
import com.salesforce.apollo.utils.BbBackedInputStream;

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

    protected volatile BlockingQueue<ByteString> ds;
    protected final Map<Digest, Member>          nextAssembly;
    protected final Digest                       nextViewId;
    protected final Transitions                  transitions;
    protected final ViewContext                  view;

    private final SliceIterator<Terminal> committee;
    private final Controller              controller;
    private final ContextGossiper         coordinator;
    private final Map<Digest, Proposed>   proposals = new ConcurrentHashMap<>();
    private final Map<Member, Join>       slate     = new ConcurrentHashMap<>();

    public ViewAssembly(Digest nextViewId, ViewContext vc, CommonCommunications<Terminal, ?> comms) {
        view = vc;
        this.nextViewId = nextViewId;
        ds = new LinkedBlockingQueue<>();
        nextAssembly = Committee.viewMembersOf(nextViewId, params().context()).stream()
                                .collect(Collectors.toMap(m -> m.getId(), m -> m));
        committee = new SliceIterator<Terminal>("Committee for " + nextViewId, params().member(),
                                                new ArrayList<>(nextAssembly.values()), comms, params().dispatcher());
        // Create a new context for reconfiguration
        final Digest reconPrefixed = view.context().getId().xor(nextViewId);
        Context<Member> reContext = new Context<Member>(reconPrefixed, 0.33, view.context().activeMembers().size());
        reContext.activate(view.context().activeMembers());

        final Fsm<Reconfiguration, Transitions> fsm = Fsm.construct(this, Transitions.class, Reconfigure.GATHER, true);
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
                                          params().dispatcher(), params().metrics());

        log.debug("View Assembly from: {} to: {} recontext: {} next assembly: {} on: {}", view.context().getId(),
                 nextViewId, reContext.getId(), nextAssembly.keySet(), params().member());
    }

    @Override
    public void certify() {
        ds.clear();
        try {
            log.debug("Certifying Join proposals of: {} count: {} on: {}", nextViewId, proposals.size(),
                      params().member());
            assert ds.size() == 0 : "Existing data! size: " + ds.size();
            ds.put(Reassemble.newBuilder()
                             .setValidations(Validations.newBuilder()
                                                        .addAllValidations(proposals.values().stream().map(p -> {
                                                            return view.generateValidation(p.vm);
                                                        }).toList()))
                             .build().toByteString());
            for (int i = 0; i < params().producer().ethereal().getEpochLength() * 2; i++) {
                ds.put(ByteString.EMPTY);
            }
        } catch (InterruptedException e) {
            log.error("Failed enqueing Join proposal of: {} on: {}", nextViewId, params().member(), e);
            transitions.failed();
        }
    }

    @Override
    public void complete() {
        log.debug("View Assembly: {} completed with: {} members on: {}", nextViewId, slate.size(), params().member());
    }

    @Override
    public void elect() {
        proposals.values().stream().filter(p -> p.validations.size() > params().toleranceLevel())
                 .sorted(Comparator.comparing(p -> p.member.getId())).forEach(p -> slate.put(p.member(), joinOf(p)));
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
        JoinRequest request = JoinRequest.newBuilder().setContext(params().context().getId().toDigeste())
                                         .setNextView(nextViewId.toDigeste()).build();
        AtomicBoolean proceed = new AtomicBoolean(true);
        AtomicReference<Runnable> reiterate = new AtomicReference<>();
        AtomicInteger countDown = new AtomicInteger(3); // 3 rounds of attempts
        reiterate.set(() -> committee.iterate((term, m) -> {
            log.trace("Requesting Join from: {} on: {}", term.getMember().getId(), params().member());
            return term.join(request);
        }, (futureSailor, term, m) -> consider(futureSailor, term, m, proceed),
                                              () -> completeSlice(proceed, reiterate, countDown)));
        reiterate.get().run();
    }

    public Map<Member, Join> getSlate() {
        final var c = slate;
        return c;
    }

    @Override
    public void nominate() {
        ds.clear();
        try {
            log.debug("Nominating proposal for: {} members: {} on: {}", nextViewId,
                      proposals.values().stream().map(p -> p.member.getId()).toList(), params().member());
            ds.put(Reassemble.newBuilder()
                             .setViewMembers(ViewMembers.newBuilder()
                                                        .addAllMembers(proposals.values().stream().map(p -> p.vm)
                                                                                .toList())
                                                        .build())
                             .build().toByteString());
            for (int i = 0; i < params().producer().ethereal().getEpochLength() * 2; i++) {
                ds.put(ByteString.EMPTY);
            }
        } catch (InterruptedException e) {
            log.error("Failed enqueing view members proposal of: {} on: {}", nextViewId, params().member(), e);
            transitions.failed();
        }
    }

    public Parameters params() {
        return view.params();
    }

    public void start() {
        ds.clear();
        coordinator.start(params().producer().gossipDuration(), params().scheduler());
        controller.start();
        transitions.fsm().enterStartState();
    }

    public void stop() {
        coordinator.stop();
        controller.stop();
    }

    protected int epochs() {
        return 3;
    }

    protected void validate(Validate v) {
        final var cid = Digest.from(v.getWitness().getId());
        var certifier = view.context().getMember(cid);
        if (certifier == null) {
            log.trace("Unknown certifier: {} on: {}", cid, params().member());
            return; // do not have the join yet
        }
        final var hash = Digest.from(v.getHash());
        final var member = nextAssembly.get(hash);
        if (member == null) {
            return;
        }
        var proposed = proposals.get(hash);
        if (proposed == null) {
            log.trace("Invalid certification, unknown view join: {} on: {}", hash, params().member());
            return; // do not have the join yet
        }
        if (!view.validate(proposed.vm, v)) {
            log.trace("Invalid cetification for view join: {} from: {} on: {}", hash,
                      Digest.from(v.getWitness().getId()), params().member());
            return;
        }
        proposed.validations.put(certifier, v);
        log.debug("Validation of view member: {}:{} using certifier: {} on: {}", member.getId(), hash,
                  certifier.getId(), params().member());
    }

    private void completeSlice(AtomicBoolean proceed, AtomicReference<Runnable> reiterate, AtomicInteger countDown) {
        final var count = proposals.size();
        if (count == nextAssembly.size()) {
            proceed.set(false);
            log.trace("Proposal assembled: {} on: {}", nextViewId, params().member());
            transitions.gathered();
            return;
        }

        if (countDown.decrementAndGet() >= 0) {
            log.trace("Retrying, attempting full assembly of: {} gathered: {} desired: {} on: {}", nextViewId, count,
                      nextAssembly.size(), params().member());
            proceed.set(true);
            reiterate.get().run();
            return;
        }

        if (count > params().toleranceLevel()) {
            proceed.set(false);
            log.trace("Proposal assembled: {} gathered: {} out of: {} on: {}", nextViewId, count, nextAssembly.size(),
                      params().member());
            transitions.gathered();
            return;
        }
        log.trace("Proposal incomplete of: {} gathered: {} required: {}, retrying on: {}", nextViewId, count,
                  params().toleranceLevel(), params().member());
        proceed.set(true);
        reiterate.get().run();
    }

    private boolean consider(Optional<ListenableFuture<ViewMember>> futureSailor, Terminal term, Member m,
                             AtomicBoolean proceed) {
        if (futureSailor.isEmpty()) {
            log.debug("No join reply from: {} on: {}", term.getMember().getId(), params().member().getId());
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
            log.debug("Error join response from: {} on: {}", term.getMember().getId(), params().member().getId(),
                      e.getCause());
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
                try {
                    final var current = ds;
                    final var take = current.take();
                    return take;
                } catch (InterruptedException e) {
                    return null;
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
        final List<Certification> witnesses = candidate.validations.values().stream().map(v -> v.getWitness())
                                                                   .sorted(Comparator.comparing(c -> new Digest(c.getId())))
                                                                   .collect(Collectors.toList());
        return Join.newBuilder().setMember(candidate.vm).setView(nextViewId.toDigeste()).addAllEndorsements(witnesses)
                   .build();
    }

    private void process(PreBlock preblock, boolean last) {
        log.trace("Creating preblock: {} last: {} on: {}",
                  params().digestAlgorithm().digest(BbBackedInputStream.aggregate(preblock.data())), last,
                  params().member());
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
