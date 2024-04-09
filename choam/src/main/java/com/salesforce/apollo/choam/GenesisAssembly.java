/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Genesis;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock.NullBlock;
import com.salesforce.apollo.choam.support.OneShot;
import com.salesforce.apollo.context.StaticContext;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.proto.PubKey;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.membership.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.salesforce.apollo.cryptography.QualifiedBase64.publicKey;
import static com.salesforce.apollo.cryptography.QualifiedBase64.signature;

/**
 * Construction of the genesis block
 *
 * @author hal.hildebrand
 */
public class GenesisAssembly implements Genesis {
    private static final Logger                log       = LoggerFactory.getLogger(GenesisAssembly.class);
    private final        Ethereal              controller;
    private final        ChRbcGossip           coordinator;
    private final        SignedViewMember      genesisMember;
    private final        Map<Digest, Member>   nextAssembly;
    private final        Map<Digest, Proposed> proposals = new ConcurrentHashMap<>();
    private final        AtomicBoolean         published = new AtomicBoolean();
    private final        Map<Member, Join>     slate     = new ConcurrentHashMap<>();
    private final        AtomicBoolean         started   = new AtomicBoolean();
    private final        Transitions           transitions;
    private final        ViewContext           view;
    private final        Map<Member, Validate> witnesses = new ConcurrentHashMap<>();
    private final        OneShot               ds;
    private volatile     Thread                blockingThread;
    private volatile     HashedBlock           reconfiguration;

    public GenesisAssembly(ViewContext vc, CommonCommunications<Terminal, ?> comms, SignedViewMember genesisMember,
                           String label) {
        view = vc;
        ds = new OneShot();
        nextAssembly = Committee.viewMembersOf(view.context().getId(), view.pendingView())
                                .stream()
                                .collect(Collectors.toMap(Member::getId, m -> m));
        if (!Dag.validate(nextAssembly.size())) {
            throw new IllegalStateException("Invalid BFT cardinality: " + nextAssembly.size());
        }
        this.genesisMember = genesisMember;

        // Create a new context for reconfiguration
        final Digest reconPrefixed = view.context().getId().prefix("Genesis Assembly");
        var reContext = new StaticContext<>(reconPrefixed, view.context().getProbabilityByzantine(), 3,
                                            view.context().getAllMembers(), view.context().getEpsilon(),
                                            view.context().size());

        final Fsm<Genesis, Transitions> fsm = Fsm.construct(this, Transitions.class, BrickLayer.INITIAL, true);
        this.transitions = fsm.getTransitions();

        fsm.setName("Genesis%s on: %s".formatted(view.context().getId(), params().member().getId()));

        Config.Builder config = params().producer().ethereal().clone();

        // Canonical assignment of members -> pid for Ethereal
        Short pid = view.roster().get(params().member().getId());
        if (pid == null) {
            config.setPid((short) 0).setnProc((short) 1);
        } else {
            config.setPid(pid).setnProc((short) view.roster().size());
        }
        config.setEpochLength(7).setNumberOfEpochs(3);
        config.setLabel("Genesis Assembly" + view.context().getId() + " on: " + params().member().getId());
        controller = new Ethereal(config.build(), params().producer().maxBatchByteSize(), dataSource(),
                                  transitions::process, transitions::nextEpoch, label);
        coordinator = new ChRbcGossip(reContext, params().member(), controller.processor(), params().communications(),
                                      params().metrics() == null ? null : params().metrics().getGensisMetrics());
        log.debug("Genesis Assembly: {} recontext: {} next assembly: {} on: {}", view.context().getId(),
                  reContext.getId(), nextAssembly.keySet(), params().member().getId());
    }

    @Override
    public void certify() {
        proposals.values()
                 .stream()
                 .filter(p -> p.certifications.size() >= params().majority())
                 .forEach(p -> slate.put(p.member(), joinOf(p)));
        assert !slate.isEmpty() : "Slate is empty, no certifications";
        reconfiguration = new HashedBlock(params().digestAlgorithm(), view.genesis(slate, view.context().getId(),
                                                                                   new NullBlock(
                                                                                   params().digestAlgorithm())));
        var validate = view.generateValidation(reconfiguration);
        log.debug("Certifying genesis block: {} for: {} count: {} on: {}", reconfiguration.hash, view.context().getId(),
                  slate.size(), params().member().getId());
        ds.setValue(validate.toByteString());
    }

    @Override
    public void certify(List<ByteString> preblock, boolean last) {
        preblock.stream().map(bs -> {
            try {
                return Validate.parseFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                return null;
            }
        }).filter(Objects::nonNull).filter(v -> !v.equals(Validate.getDefaultInstance())).forEach(this::certify);
    }

    @Override
    public void gather() {
        log.info("Gathering next assembly on: " + params().member().getId());
        var certification = view.generateValidation(genesisMember).getWitness();
        var join = Join.newBuilder()
                       .setMember(genesisMember)
                       .addEndorsements(certification)
                       .setKerl(params().kerl().get())
                       .build();
        var proposed = new Proposed(join, params().member());
        proposed.certifications.put(params().member(), certification);
        proposals.put(params().member().getId(), proposed);

        ds.setValue(join.toByteString());
        coordinator.start(params().producer().gossipDuration());
        controller.start();
    }

    @Override
    public void gather(List<ByteString> preblock, boolean last) {
        preblock.stream()
                .map(bs -> {
                    try {
                        return Join.parseFrom(bs);
                    } catch (InvalidProtocolBufferException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .filter(j -> !j.equals(Join.getDefaultInstance()))
                .peek(j -> log.info("Gathering: {} on: {}", Digest.from(j.getMember().getVm().getId()),
                                    params().member().getId()))
                .forEach(this::join);
    }

    @Override
    public void nominate() {
        var validations = Validations.newBuilder();
        proposals.values()
                 .stream()
                 .filter(p -> !p.member.equals(params().member()))
                 .map(p -> view.generateValidation(p.join.getMember()))
                 .forEach(validations::addValidations);
        ds.setValue(validations.build().toByteString());
        log.info("Nominations of: {} validations: {} on: {}", params().context().getId(),
                 validations.getValidationsCount(), params().member().getId());
    }

    @Override
    public void nominations(List<ByteString> preblock, boolean last) {
        preblock.stream()
                .map(bs -> {
                    try {
                        return Validations.parseFrom(bs);
                    } catch (InvalidProtocolBufferException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .flatMap(vs -> vs.getValidationsList().stream())
                .filter(v -> !v.equals(Validate.getDefaultInstance()))
                .forEach(this::validate);
    }

    @Override
    public void publish() {
        if (witnesses.size() < params().majority()) {
            log.trace("Cannot publish genesis: {} with: {} witnesses on: {}", reconfiguration.hash, witnesses.size(),
                      params().member().getId());
            return;
        }
        if (!published.compareAndSet(false, true)) {
            log.trace("already published genesis: {} with {} witnesses on: {}", reconfiguration.hash, witnesses.size(),
                      params().member().getId());
            return;
        }
        var b = CertifiedBlock.newBuilder().setBlock(reconfiguration.block);
        witnesses.entrySet()
                 .stream()
                 .sorted(Comparator.comparing(e -> e.getKey().getId()))
                 .map(Map.Entry::getValue)
                 .forEach(v -> b.addCertifications(v.getWitness()));
        view.publish(new HashedCertifiedBlock(params().digestAlgorithm(), b.build()));
        //        controller.completeIt();
        log.info("Genesis block: {} published with {} witnesses for: {} on: {}", reconfiguration.hash, witnesses.size(),
                 view.context().getId(), params().member().getId());
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        transitions.fsm().enterStartState();
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.trace("Stopping genesis assembly: {} on: {}", view.context().getId(), params().member().getId());
        coordinator.stop();
        controller.stop();
        final var cur = blockingThread;
        blockingThread = null;
        if (cur != null) {
            cur.interrupt();
        }
    }

    private void certify(Validate v) {
        log.trace("Validating reconfiguration block: {} height: {} on: {}", reconfiguration.hash,
                  reconfiguration.height(), params().member().getId());
        if (!view.validate(reconfiguration, v)) {
            log.warn("Cannot validate reconfiguration block: {} produced on: {}", reconfiguration.hash,
                     params().member().getId());
            return;
        }
        var member = view.context().getMember(Digest.from(v.getWitness().getId()));
        if (member != null) {
            witnesses.put(member, v);
            publish();
        }
    }

    private DataSource dataSource() {
        return () -> {
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
        };
    }

    private void join(Join join) {
        final var svm = join.getMember();
        final var mid = Digest.from(svm.getVm().getId());
        final var m = nextAssembly.get(mid);
        if (m == null) {
            log.warn("Invalid view member: {} on: {}", ViewContext.print(svm, params().digestAlgorithm()),
                     params().member().getId());
            return;
        }
        if (m.equals(params().member())) {
            return; // Don't process ourselves
        }
        final var viewId = Digest.from(svm.getVm().getView());
        if (!viewId.equals(params().genesisViewId())) {
            log.warn("Invalid view id for member: {} on: {}", ViewContext.print(svm, params().digestAlgorithm()),
                     params().member().getId());
            return;
        }

        if (!m.verify(signature(svm.getSignature()), svm.getVm().toByteString())) {
            log.warn("Could not verify view member: {} on: {}", ViewContext.print(svm, params().digestAlgorithm()),
                     params().member().getId());
            return;
        }

        PubKey encoded = svm.getVm().getConsensusKey();

        if (!m.verify(signature(svm.getVm().getSignature()), encoded.toByteString())) {
            log.warn("Could not verify consensus key from view member: {} on: {}",
                     ViewContext.print(svm, params().digestAlgorithm()), params().member().getId());
            return;
        }

        PublicKey consensusKey = publicKey(encoded);
        if (consensusKey == null) {
            log.warn("Could not deserialize consensus key from view member: {} on: {}",
                     ViewContext.print(svm, params().digestAlgorithm()), params().member().getId());
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace("Valid view member: {} on: {}", ViewContext.print(svm, params().digestAlgorithm()),
                      params().member().getId());
        }
        var proposed = proposals.computeIfAbsent(mid, k -> new Proposed(join, m));
        if (join.getEndorsementsList().size() == 1) {
            proposed.certifications.computeIfAbsent(m, k -> join.getEndorsements(0));
        }
    }

    private Join joinOf(Proposed candidate) {
        final List<Certification> witnesses = candidate.certifications.values()
                                                                      .stream()
                                                                      .sorted(
                                                                      Comparator.comparing(c -> new Digest(c.getId())))
                                                                      .collect(Collectors.toList());
        return Join.newBuilder(candidate.join).clearEndorsements().addAllEndorsements(witnesses).build();
    }

    private Parameters params() {
        return view.params();
    }

    private void validate(Validate v) {
        final var cid = Digest.from(v.getWitness().getId());
        var certifier = view.context().getMember(cid);
        if (certifier == null) {
            log.warn("Unknown certifier: {} on: {}", cid, params().member().getId());
            return; // do not have the join yet
        }
        final var vid = Digest.from(v.getHash());
        final var member = nextAssembly.get(vid);
        if (member == null) {
            return;
        }
        var proposed = proposals.get(vid);
        if (proposed == null) {
            log.warn("Invalid certification, unknown view join: {} on: {}", vid, params().member().getId());
            return; // do not have the join yet
        }
        if (!view.validate(proposed.join.getMember(), v)) {
            log.warn("Invalid certification for view join: {} from: {} on: {}", vid,
                     Digest.from(v.getWitness().getId()), params().member().getId());
            return;
        }
        var prev = proposed.certifications.put(certifier, v.getWitness());
        if (prev == null) {
            log.debug("New validation of view member: {} hash: {} using certifier: {} witnesses: {} on: {}",
                      member.getId(), vid, certifier.getId(), proposed.certifications.values().size(),
                      params().member().getId());
        } else {
            log.debug("Redundant validation of view member: {} hash: {} using certifier: {} on: {}", member.getId(),
                      vid, certifier.getId(), params().member().getId());
        }
    }

    private record Proposed(Join join, Member member, Map<Member, Certification> certifications) {
        public Proposed(Join join, Member member) {
            this(join, member, new HashMap<>());
        }
    }
}
