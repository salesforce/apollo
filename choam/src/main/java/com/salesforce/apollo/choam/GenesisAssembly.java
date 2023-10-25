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
import com.salesfoce.apollo.choam.proto.*;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.fsm.Genesis;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock.NullBlock;
import com.salesforce.apollo.choam.support.OneShot;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

/**
 * Construction of the genesis block
 *
 * @author hal.hildebrand
 */
public class GenesisAssembly implements Genesis {
    private static final Logger                log       = LoggerFactory.getLogger(GenesisAssembly.class);
    private final        Ethereal              controller;
    private final        ChRbcGossip           coordinator;
    private final        ViewMember            genesisMember;
    private final        Map<Digest, Member>   nextAssembly;
    private final        Map<Digest, Proposed> proposals = new ConcurrentHashMap<>();
    private final        AtomicBoolean         published = new AtomicBoolean();
    private final        Map<Member, Join>     slate     = new ConcurrentHashMap<>();
    private final        AtomicBoolean         started   = new AtomicBoolean();
    private final        Transitions           transitions;
    private final        ViewContext           view;
    private final        Map<Member, Validate> witnesses = new ConcurrentHashMap<>();
    private volatile     Thread                blockingThread;
    private volatile     OneShot               ds;
    private volatile     HashedBlock           reconfiguration;

    public GenesisAssembly(ViewContext vc, CommonCommunications<Terminal, ?> comms, ViewMember genesisMember,
                           ThreadPoolExecutor executor) {
        view = vc;
        ds = new OneShot();
        nextAssembly = Committee.viewMembersOf(view.context().getId(), params().context())
                                .stream()
                                .collect(Collectors.toMap(m -> m.getId(), m -> m));
        if (!Dag.validate(nextAssembly.size())) {
            log.error("Invalid cardinality: {} for: {} on: {}", nextAssembly.size(), view.context().getId(),
                      params().member().getId());
            throw new IllegalStateException("Invalid BFT cardinality: " + nextAssembly.size());
        }
        this.genesisMember = genesisMember;

        // Create a new context for reconfiguration
        final Digest reconPrefixed = view.context().getId().prefix("Genesis Assembly");
        Context<Member> reContext = new ContextImpl<Member>(reconPrefixed, view.context().memberCount(),
                                                            view.context().getProbabilityByzantine(),
                                                            view.context().getBias());
        reContext.activate(view.context().activeMembers());

        final Fsm<Genesis, Transitions> fsm = Fsm.construct(this, Transitions.class, BrickLayer.INITIAL, true);
        this.transitions = fsm.getTransitions();
        fsm.setName("Genesis" + params().member().getId());

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
                                  (preblock, last) -> transitions.process(preblock, last),
                                  epoch -> transitions.nextEpoch(epoch), executor);
        coordinator = new ChRbcGossip(reContext, params().member(), controller.processor(), params().communications(),
                                      Executors.newVirtualThreadPerTaskExecutor(),
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
        reconfiguration = new HashedBlock(params().digestAlgorithm(), view.genesis(slate, view.context().getId(),
                                                                                   new NullBlock(
                                                                                   params().digestAlgorithm())));
        var validate = view.generateValidation(reconfiguration);
        log.trace("Certifying genesis block: {} for: {} count: {} on: {}", reconfiguration.hash, view.context().getId(),
                  slate.size(), params().member().getId());
        ds = new OneShot();
        ds.setValue(validate.toByteString());
    }

    @Override
    public void certify(PreBlock preblock, boolean last) {
        preblock.data().stream().map(bs -> {
            try {
                return Validate.parseFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                return null;
            }
        }).filter(v -> v != null).filter(v -> !v.equals(Validate.getDefaultInstance())).forEach(v -> certify(v));
    }

    @Override
    public void gather() {
        var certification = view.generateValidation(genesisMember).getWitness();
        var join = Join.newBuilder()
                       .setMember(genesisMember)
                       .addEndorsements(certification)
                       .setView(view.context().getId().toDigeste())
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
    public void gather(PreBlock preblock, boolean last) {
        preblock.data().stream().map(bs -> {
            try {
                return Join.parseFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                return null;
            }
        }).filter(j -> j != null).filter(j -> !j.equals(Join.getDefaultInstance())).forEach(j -> join(j));
    }

    @Override
    public void nominate() {
        ds = new OneShot();
        var validations = Validations.newBuilder();
        proposals.values()
                 .stream()
                 .filter(p -> !p.member.equals(params().member()))
                 .map(p -> view.generateValidation(p.join.getMember()))
                 .forEach(v -> validations.addValidations(v));
        ds.setValue(validations.build().toByteString());
    }

    @Override
    public void nominations(PreBlock preblock, boolean last) {
        preblock.data()
                .stream()
                .map(bs -> {
                    try {
                        return Validations.parseFrom(bs);
                    } catch (InvalidProtocolBufferException e) {
                        return null;
                    }
                })
                .filter(v -> v != null)
                .flatMap(vs -> vs.getValidationsList().stream())
                .filter(v -> !v.equals(Validate.getDefaultInstance()))
                .forEach(v -> validate(v));
    }

    @Override
    public void publish() {
        var b = CertifiedBlock.newBuilder().setBlock(reconfiguration.block);
        witnesses.entrySet()
                 .stream()
                 .sorted(Comparator.comparing(e -> e.getKey().getId()))
                 .map(e -> e.getValue())
                 .forEach(v -> b.addCertifications(v.getWitness()));
        view.publish(new HashedCertifiedBlock(params().digestAlgorithm(), b.build()));
        log.debug("Genesis block: {} published for: {} on: {}", reconfiguration.hash, view.context().getId(),
                  params().member().getId());
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
            if (witnesses.size() >= params().majority()) {
                if (published.compareAndSet(false, true)) {
                    publish();
                }
            }
        }
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

    private void join(Join join) {
        final var vm = join.getMember();
        final var mid = Digest.from(vm.getId());
        final var m = nextAssembly.get(mid);
        if (m == null) {
            if (log.isTraceEnabled()) {
                log.trace("Invalid view member: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
                          params().member().getId());
            }
            return;
        }
        if (m.equals(params().member())) {
            return; // Don't process ourselves
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
        if (log.isTraceEnabled()) {
            log.trace("Valid view member: {} on: {}", ViewContext.print(vm, params().digestAlgorithm()),
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
        final var hash = Digest.from(v.getHash());
        final var member = nextAssembly.get(hash);
        if (member == null) {
            return;
        }
        var proposed = proposals.get(hash);
        if (proposed == null) {
            log.warn("Invalid certification, unknown view join: {} on: {}", hash, params().member().getId());
            return; // do not have the join yet
        }
        if (!view.validate(proposed.join.getMember(), v)) {
            log.warn("Invalid cetification for view join: {} from: {} on: {}", hash,
                     Digest.from(v.getWitness().getId()), params().member().getId());
            return;
        }
        proposed.certifications.put(certifier, v.getWitness());
        log.debug("Validation of view member: {}:{} using certifier: {} on: {}", member.getId(), hash,
                  certifier.getId(), params().member().getId());
    }

    private record Proposed(Join join, Member member, Map<Member, Certification> certifications) {
        public Proposed(Join join, Member member) {
            this(join, member, new HashMap<>());
        }
    }
}
