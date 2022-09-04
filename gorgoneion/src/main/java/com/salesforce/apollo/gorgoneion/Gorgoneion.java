/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.gorgoneion.proto.Deny;
import com.salesfoce.apollo.gorgoneion.proto.Endorsement;
import com.salesfoce.apollo.gorgoneion.proto.Gossip;
import com.salesfoce.apollo.gorgoneion.proto.Have;
import com.salesfoce.apollo.gorgoneion.proto.Nonce;
import com.salesfoce.apollo.gorgoneion.proto.Pending;
import com.salesfoce.apollo.gorgoneion.proto.Proposal;
import com.salesfoce.apollo.gorgoneion.proto.Registration;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedDeny;
import com.salesfoce.apollo.gorgoneion.proto.SignedEndorsement;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.gorgoneion.proto.SignedProposal;
import com.salesfoce.apollo.gorgoneion.proto.Update;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingCommunications.Destination;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.thoth.grpc.admission.Admission;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionClient;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionServer;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionService;
import com.salesforce.apollo.thoth.grpc.admission.gossip.Replication;
import com.salesforce.apollo.thoth.grpc.admission.gossip.ReplicationClient;
import com.salesforce.apollo.thoth.grpc.admission.gossip.ReplicationServer;
import com.salesforce.apollo.thoth.grpc.admission.gossip.ReplicationService;
import com.salesforce.apollo.thoth.metrics.GorgoneionMetrics;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomWindow;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Apollo attested admission service
 *
 * @author hal.hildebrand
 *
 */
public class Gorgoneion {
    public record Parameters(double fpr, SecureRandom entropy,
                             Function<SignedAttestation, CompletableFuture<Boolean>> verifier, Clock clock,
                             Executor exec, DigestAlgorithm digestAlgo, Duration registrationTimeout, int maxTracked) {

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private Clock                                                   clock               = Clock.systemUTC();
            private DigestAlgorithm                                         digestAlgo          = DigestAlgorithm.DEFAULT;
            private SecureRandom                                            entropy;
            private Executor                                                exec                = r -> r.run();
            private double                                                  fpr                 = 0.000125;
            private int                                                     maxTracked          = 100;
            private Duration                                                registrationTimeout = Duration.ofSeconds(30);
            private Function<SignedAttestation, CompletableFuture<Boolean>> verifier;

            public Parameters build() {
                return new Parameters(fpr, entropy, verifier, clock, exec, digestAlgo, registrationTimeout, maxTracked);
            }

            public Clock getClock() {
                return clock;
            }

            public DigestAlgorithm getDigestAlgo() {
                return digestAlgo;
            }

            public SecureRandom getEntropy() {
                return entropy;
            }

            public Executor getExec() {
                return exec;
            }

            public double getFpr() {
                return fpr;
            }

            public int getMaxTracked() {
                return maxTracked;
            }

            public Duration getRegistrationTimeout() {
                return registrationTimeout;
            }

            public Function<SignedAttestation, CompletableFuture<Boolean>> getVerifier() {
                return verifier;
            }

            public Builder setClock(Clock clock) {
                this.clock = clock;
                return this;
            }

            public Builder setDigestAlgo(DigestAlgorithm digestAlgo) {
                this.digestAlgo = digestAlgo;
                return this;
            }

            public Builder setEntropy(SecureRandom entropy) {
                this.entropy = entropy;
                return this;
            }

            public Builder setExec(Executor exec) {
                this.exec = exec;
                return this;
            }

            public Builder setFpr(double fpr) {
                this.fpr = fpr;
                return this;
            }

            public Builder setMaxTracked(int maxTracked) {
                this.maxTracked = maxTracked;
                return this;
            }

            public Builder setRegistrationTimeout(Duration registrationTimeout) {
                this.registrationTimeout = registrationTimeout;
                return this;
            }

            public Builder setVerifier(Function<SignedAttestation, CompletableFuture<Boolean>> verifier) {
                this.verifier = verifier;
                return this;
            }
        }
    }

    private class Admissions implements Admission {
        @Override
        public CompletableFuture<SignedNonce> apply(Registration request, Digest from) {
            if (!validate(request, from)) {
                var fs = new CompletableFuture<SignedNonce>();
                fs.complete(SignedNonce.getDefaultInstance());
                return fs;
            }
            return generateNonce(request);
        }

        @Override
        public void register(SignedAttestation request, Digest from, StreamObserver<Validations> observer) {
            if (!validate(request, from)) {
                observer.onError(new StatusRuntimeException(io.grpc.Status.UNAUTHENTICATED));
                return;
            }
            Gorgoneion.this.register(request, from, observer);
        }

        private boolean validate(Registration registration, Digest from) {
            return true;
        }

        private boolean validate(SignedAttestation request, Digest from) {
            return true;
        }
    }

    private class State implements Replication {

        private final ConcurrentNavigableMap<Digest, SignedDeny>                               denies       = new ConcurrentSkipListMap<>();
        private Duration                                                                       durationPerRound;
        private final ConcurrentNavigableMap<Digest, SignedEndorsement>                        endorsements = new ConcurrentSkipListMap<>();
        private volatile ScheduledFuture<?>                                                    futureGossip;
        private final RingCommunications<Member, ReplicationService>                  gossiper;
        private final ConcurrentNavigableMap<SelfAddressingIdentifier, Pending>                pending      = new ConcurrentSkipListMap<>();
        private final BloomWindow<Digest>                                                      processed;
        private final ConcurrentNavigableMap<SelfAddressingIdentifier, SignedProposal>         proposals    = new ConcurrentSkipListMap<>();
        private final CommonCommunications<ReplicationService, Replication> replicationComms;
        private final RoundScheduler                                                           roundTimers;
        private final ConcurrentNavigableMap<SelfAddressingIdentifier, Votes>                  votes        = new ConcurrentSkipListMap<>();

        private State(Router replicationRouter, GorgoneionMetrics metrics) {
            replicationComms = replicationRouter.create(member, context.getId(), this, "replication",
                                                        r -> new ReplicationServer(r,
                                                                                             replicationRouter.getClientIdentityProvider(),
                                                                                             parameters.exec, metrics),
                                                        ReplicationClient.getCreate(context.getId(), metrics),
                                                        ReplicationClient.getLocalLoopback(this, member));
            gossiper = new RingCommunications<>(context, member, replicationComms, parameters.exec);
            processed = new BloomWindow<>(parameters.maxTracked,
                                          () -> new DigestBloomFilter(Entropy.nextBitsStreamLong(),
                                                                      parameters.maxTracked, 0.000125),
                                          3);
            roundTimers = new RoundScheduler("replications", context.timeToLive());
        }

        @Override
        public Update gossip(Gossip gossip, Digest from) {
            return state.updateFor(gossip.getHave()).setHave(state.getHave()).build();
        }

        @Override
        public void update(Update update, Digest from) {
            update(update, from);
        }

        private void add(Pending p) {
            final var identifier = validate(p);
            if (identifier == null) {
                return;
            }
            pending.computeIfAbsent(identifier, d -> {
                roundTimers.schedule(() -> {
                    gcPending(p);
                }, roundsFor(p));
                return p;
            });
        }

        private void add(SignedDeny c) {
            final var digest = validate(c);
            if (digest == null) {
                return;
            }
            denies.putIfAbsent(digest, c);
        }

        private void add(SignedEndorsement c) {
            final var digest = validate(c);
            if (digest == null) {
                return;
            }
            endorsements.computeIfAbsent(digest, d -> {
                final var identifier = identifier(c.getEndorsement().getNonce().getNonce().getMember());
                if (identifier == null) {
                    return null;
                }
                votes.computeIfAbsent(identifier, null);
                return c;
            });
        }

        private void add(SignedProposal p) {
            final var identifier = validate(p);
            if (identifier == null) {
                return;
            }
            proposals.computeIfAbsent(identifier, d -> {
                maybeEndorse(p);
                return p;
            });
        }

        private Have getHave() {
            return Have.newBuilder()
                       .setEndorsements(haveEndorsements())
                       .setDenies(haveDenies())
                       .setPending(havePending())
                       .setProposals(haveProposals())
                       .build();
        }

        private ListenableFuture<Update> gossip(ReplicationService link, Integer ring) {
            if (!started.get()) {
                return null;
            }
            roundTimers.tick();
            return link.gossip(Gossip.newBuilder().setHave(getHave()).build());
        }

        private void gossip(Duration frequency, ScheduledExecutorService scheduler) {
            if (!started.get()) {
                return;
            }
            parameters.exec.execute(Utils.wrapped(() -> {
                if (context.activeCount() == 1) {
                    roundTimers.tick();
                }
                gossiper.execute((link, ring) -> gossip(link, ring),
                                 (futureSailor, destination) -> gossip(futureSailor, destination, frequency,
                                                                       scheduler));
            }, log));
        }

        private void gossip(Optional<ListenableFuture<Update>> futureSailor,
                            Destination<Member, ReplicationService> destination, Duration frequency,
                            ScheduledExecutorService scheduler) {
            final var member = destination.member();
            try {
                if (futureSailor.isEmpty()) {
                    return;
                }
                try {
                    final var adminUpdate = futureSailor.get().get();
                    process(adminUpdate);
                    destination.link().update(updateFor(adminUpdate.getHave()).build());
                } catch (ExecutionException e) {
                    log.trace("Error in gossip on: {}", member.getId(), e);
                    return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } catch (Throwable e) {
                log.warn("Error in gossip on: {}", member.getId(), e);
                return;
            } finally {
                if (!started.get()) {
                    return;
                }
                futureGossip = scheduler.schedule(() -> gossip(frequency, scheduler), frequency.toNanos(),
                                                  TimeUnit.NANOSECONDS);
            }
        }

        private Biff haveDenies() {
            var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, denies.size()), parameters.fpr);
            denies.keySet().forEach(d -> bif.add(d));
            return bif.toBff();
        }

        private Biff haveEndorsements() {
            var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, endorsements.size()),
                                            parameters.fpr);
            endorsements.keySet().forEach(d -> bif.add(d));
            return bif.toBff();
        }

        private Biff havePending() {
            var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, pending.size()),
                                            parameters.fpr);
            pending.keySet().forEach(d -> bif.add(d.getDigest()));
            return bif.toBff();
        }

        private Biff haveProposals() {
            var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, proposals.size()),
                                            parameters.fpr);
            proposals.keySet().forEach(d -> bif.add(d.getDigest()));
            return bif.toBff();
        }

        private void process(Update adminUpdate) {
            adminUpdate.getEndorsementsList().forEach(c -> add(c));
            adminUpdate.getDenyList().forEach(d -> add(d));
            adminUpdate.getProposalsList().forEach(p -> add(p));
            adminUpdate.getPendingList().forEach(p -> add(p));
        }

        private long roundsFor(Pending p) {
            return duration(p.getPending().getNonce().getDuration()).dividedBy(durationPerRound);
        }

        private void stop() {
            final var current = futureGossip;
            futureGossip = null;
            if (current != null) {
                futureGossip.cancel(true);
            }
            admissionComms.deregister(context.getId());
            replicationComms.deregister(context.getId());
        }

        private List<SignedDeny> updateDenies(Biff have) {
            BloomFilter<Digest> d = BloomFilter.from(have);
            return denies.entrySet().stream().filter(e -> !d.contains(e.getKey())).map(e -> e.getValue()).toList();
        }

        private List<SignedEndorsement> updateEndorsements(Biff have) {
            BloomFilter<Digest> haves = BloomFilter.from(have);
            return endorsements.entrySet()
                               .stream()
                               .filter(e -> !haves.contains(e.getKey()))
                               .map(e -> e.getValue())
                               .toList();
        }

        private Update.Builder updateFor(final Have have) {
            return Update.newBuilder()
                         .addAllEndorsements(updateEndorsements(have.getEndorsements()))
                         .addAllDeny(updateDenies(have.getDenies()))
                         .addAllPending(updatePending(have.getPending()))
                         .addAllProposals(updateProposals(have.getProposals()));
        }

        private List<Pending> updatePending(Biff have) {
            BloomFilter<Digest> p = BloomFilter.from(have);
            return pending.entrySet()
                          .stream()
                          .filter(e -> !p.contains(e.getKey().getDigest()))
                          .map(e -> e.getValue())
                          .toList();
        }

        private List<SignedProposal> updateProposals(Biff have) {
            BloomFilter<Digest> p = BloomFilter.from(have);
            return proposals.entrySet()
                            .stream()
                            .filter(e -> !p.contains(e.getKey().getDigest()))
                            .map(e -> e.getValue())
                            .toList();
        }

        private SelfAddressingIdentifier validate(Pending c) {
            return identifier(c.getPending().getNonce().getMember());
        }

        private Digest validate(SignedDeny c) {
            final var signature = JohnHancock.from(c.getSignature());
            final var digest = signature.toDigest(parameters.digestAlgo);
            if (processed.contains(digest)) {
                return null;
            }

            final var memberId = digest(c.getDenial().getNonce().getNonce().getMember());
            final var member = context.getActiveMember(memberId);
            if ((member == null) || !member.verify(signature, c.getDenial().toByteString())) {
                return null;
            }
            return digest;
        }

        private Digest validate(SignedEndorsement c) {
            final var signature = JohnHancock.from(c.getSignature());
            final var digest = signature.toDigest(parameters.digestAlgo);
            if (processed.contains(digest)) {
                return null;
            }
            final var memberId = digest(c.getEndorsement().getNonce().getNonce().getMember());
            final var member = context.getActiveMember(memberId);
            if ((member == null) || !member.verify(signature, c.getEndorsement().toByteString())) {
                return null;
            }
            return digest;
        }

        private SelfAddressingIdentifier validate(SignedProposal c) {
            final var signature = JohnHancock.from(c.getSignature());
            final var digest = signature.toDigest(parameters.digestAlgo);
            if (processed.contains(digest)) {
                return null;
            }
            var proposerId = Digest.from(c.getProposal().getProposer());
            final var proposer = context.getActiveMember(proposerId);
            if ((proposer == null) || !proposer.verify(signature, c.getProposal().toByteString())) {
                return null;
            }

            return identifier(c.getProposal().getNonce().getNonce().getMember());
        }
    }

    private record Votes(Proposal proposal, Set<SignedEndorsement> endorsements, Set<SignedDeny> deny) {}

    private static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    private final CommonCommunications<AdmissionService, Admission>                             admissionComms;
    private final Admissions                                                                    admissions     = new Admissions();
    private final Context<Member>                                                               context;
    @SuppressWarnings("unused")
    private final KERL                                                                          kerl;
    private final ControlledIdentifierMember                                                    member;
    private final Parameters                                                                    parameters;
    private final ConcurrentNavigableMap<SelfAddressingIdentifier, Consumer<List<Validation_>>> pendingClients = new ConcurrentSkipListMap<>();
    private final AtomicBoolean                                                                 started        = new AtomicBoolean();
    private final State                                                                         state;
    private final ControlledIdentifier<SelfAddressingIdentifier>                                validating;

    public Gorgoneion(ControlledIdentifierMember member, ControlledIdentifier<SelfAddressingIdentifier> validating,
                      Context<Member> context, Router admissionsRouter, KERL kerl, Router replicationRouter,
                      Parameters parameters, GorgoneionMetrics metrics) {
        admissionComms = admissionsRouter.create(member, context.getId(), admissions, "admissions",
                                                 r -> new AdmissionServer(r,
                                                                          admissionsRouter.getClientIdentityProvider(),
                                                                          parameters.exec, metrics),
                                                 AdmissionClient.getCreate(context.getId(), metrics),
                                                 AdmissionClient.getLocalLoopback(admissions, member));
        this.parameters = parameters;
        this.member = member;
        this.context = context;
        this.kerl = kerl;
        this.validating = validating;
        this.state = new State(replicationRouter, metrics);
    }

    public void start(Duration frequency, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        state.gossip(frequency, scheduler);
    }

    public void stop() {
        if (!started.compareAndExchange(true, false)) {
            return;
        }
        state.stop();
    }

    private Digest digest(Ident identifier) {
        return ((SelfAddressingIdentifier) Identifier.from(identifier)).getDigest();
    }

    private Duration duration(com.google.protobuf.Duration duration) {
        return Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
    }

    private CompletableFuture<Validation_> endorse(Pending p) {
        var event = p.getKerl().getEvents(0).getInception();
        return validating.getSigner()
                         .thenApply(signer -> Validation_.newBuilder()
                                                         .setValidator(validating.getCoordinates().toEventCoords())
                                                         .setSignature(signer.sign(event.toByteString()).toSig())
                                                         .build());
    }

    private CompletableFuture<Endorsement> endorse(SignedProposal sp) {
        var identifier = identifier(sp.getProposal().getAttestation().getAttestation().getMember());
        var pend = state.pending.get(identifier);
        if (pend == null) {
            var fs = new CompletableFuture<Endorsement>();
            fs.complete(Endorsement.getDefaultInstance());
            return fs;
        }
        return endorse(pend).thenApply(v -> Endorsement.newBuilder()
                                                       .setEndorser(member.getId().toDigeste())
                                                       .setNonce(pend.getPending())
                                                       .setValidation(v)
                                                       .build());
    }

    private void gcPending(Pending p) {

    }

    private CompletableFuture<SignedNonce> generateNonce(Registration registration) {
        var noise = new byte[parameters.digestAlgo.longLength()];
        parameters.entropy.nextBytes(noise);
        var now = parameters.clock.instant();
        var nonce = Nonce.newBuilder()
                         .setMember(registration.getIdentity())
                         .setDuration(com.google.protobuf.Duration.newBuilder()
                                                                  .setSeconds(parameters.registrationTimeout.toSecondsPart())
                                                                  .setNanos(parameters.registrationTimeout.toNanosPart())
                                                                  .build())
                         .setIssuer(validating.getLastEstablishmentEvent().toEventCoords())
                         .setNoise(parameters.digestAlgo.random(parameters.entropy).toDigeste())
                         .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                         .build();
        return validating.getSigner().thenApply(s -> {
            final var signature = s.sign(nonce.toByteString());
            return SignedNonce.newBuilder().setNonce(nonce).setSignature(signature.toSig()).build();
        }).thenApply(signed -> {
            final var identifier = identifier(registration.getIdentity());
            if (identifier == null) {
                return null;
            }
            state.pending.put(identifier,
                              Pending.newBuilder().setPending(signed).setKerl(registration.getKerl()).build());
            return signed;
        });
    }

    private SelfAddressingIdentifier identifier(Ident identifier) {
        final var from = Identifier.from(identifier);
        if (from instanceof SelfAddressingIdentifier sai) {
            return sai;
        }
        return null;
    }

    private void maybeEndorse(SignedProposal p) {
        parameters.verifier.apply(p.getProposal().getAttestation()).thenCompose(success -> {
            if (!success) {
                var deny = Deny.newBuilder().build();
                state.add(SignedDeny.newBuilder()
                                    .setDenial(deny)
                                    .setSignature(member.sign(deny.toByteString()).toSig())
                                    .build());
                var fs = new CompletableFuture<Endorsement>();
                fs.complete(null);
                return fs;
            } else {
                return endorse(p);
            }
        }).whenComplete((endorsement, error) -> {
            if (error != null) {
                log.error("Error endorsing proposal on: {}", member.getId(), error);
            } else if (endorsement != null) {
                state.add(SignedEndorsement.newBuilder()
                                           .setEndorsement(endorsement)
                                           .setSignature(member.sign(endorsement.toByteString()).toSig())
                                           .build());
            }
        });
    }

    private void register(SignedAttestation request, Digest from, StreamObserver<Validations> observer) {
        final var identifier = identifier(request.getAttestation().getMember());
        if (identifier == null) {
            observer.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Invalid identifier")));
            return;
        }
        var pending = state.pending.get(identifier);
        if (pending == null) {
            observer.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("No pending admission")));
            return;
        }
        var proposal = Proposal.newBuilder()
                               .setNonce(pending.getPending())
                               .setAttestation(request)
                               .setProposer(member.getId().toDigeste())
                               .build();
        var signed = SignedProposal.newBuilder()
                                   .setProposal(proposal)
                                   .setSignature(member.sign(proposal.toByteString()).toSig())
                                   .build();
        pendingClients.putIfAbsent(identifier, validations -> {
            observer.onNext(Validations.newBuilder().addAllValidations(validations).build());
            observer.onCompleted();
        });
        state.add(signed);
    }
}
