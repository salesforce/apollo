/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.collect.HashMultiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.fireflies.proto.Attestation;
import com.salesfoce.apollo.fireflies.proto.Credentials;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Invitation;
import com.salesfoce.apollo.fireflies.proto.Join;
import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesfoce.apollo.fireflies.proto.Registration;
import com.salesfoce.apollo.fireflies.proto.SignedAttestation;
import com.salesfoce.apollo.fireflies.proto.SignedNonce;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.fireflies.communications.Approach;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

import io.grpc.StatusRuntimeException;

/**
 * Embodiment of the client side join protocol
 *
 */
class Binding {
    private final static Logger log = LoggerFactory.getLogger(Binding.class);

    final CommonCommunications<Approach, Service> approaches;
    private final Context<Participant>            context;
    private final DigestAlgorithm                 digestAlgo;
    private final Duration                        duration;
    private final Executor                        exec;
    private final FireflyMetrics                  metrics;
    private final Node                            node;
    private final Parameters                      params;
    private final ScheduledExecutorService        scheduler;
    private final List<Seed>                      seeds;
    private final View                            view;

    public Binding(View view, List<Seed> seeds, Duration duration, ScheduledExecutorService scheduler,
                   Context<Participant> context, CommonCommunications<Approach, Service> approaches, Node node,
                   Parameters params, FireflyMetrics metrics, Executor exec, DigestAlgorithm digestAlgo) {
        this.view = view;
        this.duration = duration;
        this.seeds = new ArrayList<>(seeds);
        this.scheduler = scheduler;
        this.context = context;
        this.node = node;
        this.params = params;
        this.metrics = metrics;
        this.exec = exec;
        this.approaches = approaches;
        this.digestAlgo = digestAlgo;
    }

    void seeding() {
        if (seeds.isEmpty()) {// This node is the bootstrap seed
            bootstrap();
            return;
        }
        Entropy.secureShuffle(seeds);
        log.info("Seeding view: {} context: {} with seeds: {} started on: {}", view.currentView(), this.context.getId(),
                 seeds.size(), node.getId());

        var seeding = new CompletableFuture<Redirect>();
        var timer = metrics == null ? null : metrics.seedDuration().time();
        seeding.whenComplete(validate(duration, scheduler, timer));

        var seedlings = new SliceIterator<>("Seedlings", node,
                                            seeds.stream()
                                                 .map(s -> seedFor(s))
                                                 .map(nw -> view.new Participant(
                                                                                 nw))
                                                 .filter(p -> !node.getId().equals(p.getId()))
                                                 .collect(Collectors.toList()),
                                            approaches, exec);
        AtomicReference<Runnable> reseed = new AtomicReference<>();
        reseed.set(() -> {
            seedlings.iterate((link, m) -> {
                log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), node.getId());
                return link.seed(registration());
            }, (futureSailor, link, m) -> complete(seeding, futureSailor, m), () -> {
                if (!seeding.isDone()) {
                    scheduler.schedule(exec(() -> reseed.get().run()), params.retryDelay().toNanos(),
                                       TimeUnit.NANOSECONDS);
                }
            }, scheduler, params.retryDelay());
        });
        reseed.get().run();
    }

    private SignedAttestation attestation(SignedNonce nonce, Any proof) {
        var now = params.gorgoneion().clock().instant();
        var attestation = Attestation.newBuilder()
                                     .setAttestation(proof)
                                     .setKerl(node.kerl())
                                     .setNonce(node.sign(nonce.toByteString()).toSig())
                                     .setTimestamp(Timestamp.newBuilder()
                                                            .setSeconds(now.getEpochSecond())
                                                            .setNanos(now.getNano()))
                                     .build();
        return SignedAttestation.newBuilder()
                                .setAttestation(attestation)
                                .setSignature(node.sign(attestation.toByteString()).toSig())
                                .build();
    }

    private void bootstrap() {
        log.info("Bootstrapping seed node view: {} context: {} on: {}", view.currentView(), this.context.getId(),
                 node.getId());
        var nw = node.getNote();
        final var sched = scheduler;
        final var dur = duration;

        view.bootstrap(nw, sched, dur);
    }

    private boolean complete(CompletableFuture<Redirect> redirect, Optional<ListenableFuture<Redirect>> futureSailor,
                             Member m) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            final var r = futureSailor.get().get();
            if (redirect.complete(r)) {
                log.info("Redirect to view: {} context: {} from: {} on: {}", Digest.from(r.getView()),
                         this.context.getId(), m.getId(), node.getId());
            }
            return false;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                case RESOURCE_EXHAUSTED:
                    log.trace("SRE in redirect: {} on: {}", sre.getStatus(), node.getId());
                    break;
                default:
                    log.trace("SRE in redirect: {} on: {}", sre.getStatus(), node.getId());
                }
            } else {
                log.error("Error in redirect: {} on: {}", ex.getCause(), node.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (CancellationException e) {
            // noop
        }
        return true;
    }

    private boolean completeGateway(Participant member, CompletableFuture<Bound> gateway,
                                    Optional<ListenableFuture<Gateway>> futureSailor, HashMultiset<Biff> memberships,
                                    HashMultiset<Digest> views, HashMultiset<Integer> cards, Set<SignedNote> seeds,
                                    Digest v, int majority, Set<SignedNote> joined) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        if (gateway.isDone()) {
            return false;
        }

        Gateway g;
        try {
            g = futureSailor.get().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                case RESOURCE_EXHAUSTED:
                    log.trace("Resource exhausted in join: {} with: {} : {} on: {}", v, member.getId(), sre.getStatus(),
                              node.getId());
                    break;
                case OUT_OF_RANGE:
                    log.debug("View change in join: {} with: {} : {} on: {}", v, member.getId(), sre.getStatus(),
                              node.getId());
                    view.resetBootstrapView();
                    node.reset();
                    exec.execute(() -> seeding());
                    return false;
                case DEADLINE_EXCEEDED:
                    log.trace("Join timeout for view: {} with: {} : {} on: {}", v, member.getId(), sre.getStatus(),
                              node.getId());
                    break;
                case UNAUTHENTICATED:
                    log.trace("Join unauthenticated for view: {} with: {} : {} on: {}", v, member.getId(),
                              sre.getStatus(), node.getId());
                    break;
                default:
                    log.warn("Failure in join: {} with: {} : {} on: {}", v, member.getId(), sre.getStatus(),
                             node.getId());
                }
            } else {
                log.error("Failure in join: {} with: {} on: {}", v, member.getId(), node.getId(), e.getCause());
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (CancellationException e) {
            return true;
        }

        if (g.equals(Gateway.getDefaultInstance())) {
            return true;
        }
        if (g.getInitialSeedSetCount() == 0) {
            log.warn("No seeds in join returned from: {} on: {}", member.getId(), node.getId());
            return true;
        }
        if (g.getMembers().equals(Biff.getDefaultInstance())) {
            log.warn("No membership in join returned from: {} on: {}", member.getId(), node.getId());
            return true;
        }
        var gatewayView = Digest.from(g.getView());
        if (gatewayView.equals(Digest.NONE)) {
            log.trace("Empty view in join returned from: {} on: {}", member.getId(), node.getId());
            return true;
        }
        views.add(gatewayView);
        cards.add(g.getCardinality());
        memberships.add(g.getMembers());
        seeds.addAll(g.getInitialSeedSetList());
        joined.addAll(g.getJoiningList());

        var vs = views.entrySet()
                      .stream()
                      .filter(e -> e.getCount() >= majority)
                      .map(e -> e.getElement())
                      .findFirst()
                      .orElse(null);
        if (vs != null) {
            if (validate(g, vs, gateway, memberships, cards, seeds, majority, joined)) {
                return false;
            }
        }
        log.debug("Gateway received, view count: {} cardinality: {} memberships: {} majority: {} from: {} view: {} context: {} on: {}",
                  views.size(), cards.size(), memberships.size(), majority, member.getId(), gatewayView,
                  this.context.getId(), node.getId());
        return true;
    }

    private boolean completeValidation(Participant from, int majority, Digest v,
                                       CompletableFuture<Validations> validated,
                                       Optional<ListenableFuture<Invitation>> futureSailor,
                                       Map<Member, Validation_> validations) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        Invitation invite;
        try {
            invite = futureSailor.get().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                case RESOURCE_EXHAUSTED:
                    log.trace("Resource exhausted in validation: {} with: {} : {} on: {}", v, from.getId(),
                              sre.getStatus(), node.getId());
                    break;
                case OUT_OF_RANGE:
                    log.debug("View change in validation: {} with: {} : {} on: {}", v, from.getId(), sre.getStatus(),
                              node.getId());
                    view.resetBootstrapView();
                    node.reset();
                    exec.execute(() -> seeding());
                    return false;
                case DEADLINE_EXCEEDED:
                    log.trace("Validation timeout for view: {} with: {} : {} on: {}", v, from.getId(), sre.getStatus(),
                              node.getId());
                    break;
                case UNAUTHENTICATED:
                    log.trace("Validation unauthenticated for view: {} with: {} : {} on: {}", v, from.getId(),
                              sre.getStatus(), node.getId());
                    break;
                default:
                    log.warn("Failure in validation: {} with: {} : {} on: {}", v, from.getId(), sre.getStatus(),
                             node.getId());
                }
            } else {
                log.error("Failure in validation: {} with: {} on: {}", v, from.getId(), node.getId(), e.getCause());
            }
            return true;
        } catch (CancellationException e) {
            return true;
        }

        final var validation = invite.getValidation();
        if (!validate(validation, from)) {
            return true;
        }
        if (validations.put(from, validation) == null) {
            if (validations.size() >= majority) {
                validated.complete(Validations.newBuilder().build());
                log.info("Validations acquired: {} majority: {} for view: {} context: {} on: {}", validations.size(),
                         majority, v, this.context.getId(), node.getId());
                return false;
            }
        }
        log.info("Validation: {} majority not achieved: {} required: {} context: {} on: {}", v, validations.size(),
                 majority, this.context.getId(), node.getId());
        return true;
    }

    private CompletableFuture<Credentials> credentials(SignedNonce nonce) {
        return params.gorgoneion()
                     .attester()
                     .apply(nonce)
                     .thenApply(attestation -> Credentials.newBuilder()
                                                          .setContext(context.getId().toDigeste())
                                                          .setKerl(node.kerl())
                                                          .setNonce(nonce)
                                                          .setAttestation(attestation(nonce, attestation))
                                                          .build());
    }

    private Runnable exec(Runnable action) {
        return () -> exec.execute(Utils.wrapped(action, log));
    }

    private BiConsumer<? super Validations, ? super Throwable> join(boolean bootstrap, Digest v, int cardinality,
                                                                    List<Participant> successors, Duration duration,
                                                                    ScheduledExecutorService scheduler, int rings) {
        return (validations, t) -> {
            if (t != null) {
                log.error("Failed validating on: {}", node.getId(), t);
                return;
            }
            if (!validations.isInitialized()) {
                log.error("Empty validations on: {}", node.getId(), t);
                return;
            }

            log.info("Rebalancing to cardinality: {} (joining) for: {} context: {} on: {}", cardinality, v,
                     context.getId(), node.getId());
            this.context.rebalance(cardinality);
            node.nextNote(v);

            log.debug("Completing validation for view: {} context: {} successors: {} on: {}", v, this.context.getId(),
                      successors.size(), node.getId());
            join(bootstrap, validations, cardinality, v, rings, successors, duration, scheduler);
        };
    }

    private void join(boolean bootstrap, Validations validations, int cardinality, Digest v, int rings,
                      List<Participant> successors, Duration duration, ScheduledExecutorService scheduler) {
        log.info("Redirecting to: {} context: {} successors: {} on: {}", v, this.context.getId(), successors.size(),
                 node.getId());
        var gateway = new CompletableFuture<Bound>();
        var timer = metrics == null ? null : metrics.joinDuration().time();
        gateway.whenComplete(view.join(scheduler, duration, timer));

        var regate = new AtomicReference<Runnable>();
        var retries = new AtomicInteger();

        HashMultiset<Biff> biffs = HashMultiset.create();
        HashSet<SignedNote> seeds = new HashSet<>();
        HashMultiset<Digest> views = HashMultiset.create();
        HashMultiset<Integer> cards = HashMultiset.create();
        Set<SignedNote> joined = new HashSet<>();

        log.info("Rebalancing to cardinality: {} (join) for: {} context: {} on: {}", cardinality, v, context.getId(),
                 node.getId());
        this.context.rebalance(cardinality);
        node.nextNote(v);

        final var redirecting = new SliceIterator<>("Gateways", node, successors, approaches, exec);
        var majority = bootstrap ? 1 : Context.minimalQuorum(rings, this.context.getBias());
        final var join = join(v);
        regate.set(() -> {
            redirecting.iterate((link, m) -> {
                log.debug("Joining: {} contacting: {} on: {}", v, link.getMember().getId(), node.getId());
                return link.join(join, params.seedingTimeout());
            }, (futureSailor, link, m) -> completeGateway((Participant) m, gateway, futureSailor, biffs, views, cards,
                                                          seeds, v, majority, joined),
                                () -> {
                                    if (retries.get() < params.joinRetries()) {
                                        log.debug("Failed to join view: {} retry: {} out of: {} on: {}", v,
                                                  retries.incrementAndGet(), params.joinRetries(), node.getId());
                                        biffs.clear();
                                        views.clear();
                                        cards.clear();
                                        seeds.clear();
                                        scheduler.schedule(exec(() -> regate.get().run()),
                                                           Entropy.nextBitsStreamLong(params.retryDelay().toNanos()),
                                                           TimeUnit.NANOSECONDS);
                                    } else {
                                        log.error("Failed to join view: {} cannot obtain majority on: {}", view,
                                                  node.getId());
                                        view.stop();
                                    }
                                }, scheduler, params.retryDelay());
        });
        regate.get().run();
    }

    private Join join(Digest v) {
        return Join.newBuilder()
                   .setContext(context.getId().toDigeste())
                   .setView(v.toDigeste())
                   .setNote(node.getNote().getWrapped())
                   .setKerl(node.kerl())
                   .build();
    }

    private Registration registration() {
        return Registration.newBuilder()
                           .setContext(context.getId().toDigeste())
                           .setView(view.bootstrapView().toDigeste())
                           .setKerl(node.kerl())
                           .setNote(node.note.getWrapped())
                           .build();
    }

    private NoteWrapper seedFor(Seed seed) {
        SignedNote seedNote = SignedNote.newBuilder()
                                        .setNote(Note.newBuilder()
                                                     .setCurrentView(view.currentView().toDigeste())
                                                     .setHost(seed.endpoint().getHostName())
                                                     .setPort(seed.endpoint().getPort())
                                                     .setCoordinates(seed.coordinates().toEventCoords())
                                                     .setEpoch(-1)
                                                     .setMask(ByteString.copyFrom(Node.createInitialMask(context)
                                                                                      .toByteArray())))
                                        .setSignature(SignatureAlgorithm.NULL_SIGNATURE.sign(null, new byte[0]).toSig())
                                        .build();
        return new NoteWrapper(seedNote, digestAlgo);
    }

    private BiConsumer<? super Redirect, ? super Throwable> validate(Duration duration,
                                                                     ScheduledExecutorService scheduler,
                                                                     Timer.Context timer) {
        return (r, t) -> {
            if (t != null) {
                log.error("Failed seeding on: {}", node.getId(), t);
                return;
            }
            if (!r.isInitialized()) {
                log.error("Empty seeding response on: {}", node.getId(), t);
                return;
            }
            var view = Digest.from(r.getView());
            log.info("Rebalancing to cardinality: {} (validate) for: {} context: {} on: {}", r.getCardinality(), view,
                     context.getId(), node.getId());
            this.context.rebalance(r.getCardinality());
            node.nextNote(view);

            log.debug("Completing redirect to view: {} context: {} successors: {} on: {}", view, this.context.getId(),
                      r.getSuccessorsCount(), node.getId());
            if (timer != null) {
                timer.close();
            }
            validate(r, duration, scheduler);
        };
    }

    private boolean validate(Gateway g, Digest v, CompletableFuture<Bound> gateway, HashMultiset<Biff> memberships,
                             HashMultiset<Integer> cards, Set<SignedNote> seeds, int majority, Set<SignedNote> joined) {
        final var max = cards.entrySet()
                             .stream()
                             .filter(e -> e.getCount() >= majority)
                             .mapToInt(e -> e.getElement())
                             .findFirst();
        var cardinality = max.orElse(-1);
        if (cardinality > 0) {
            var members = memberships.entrySet()
                                     .stream()
                                     .filter(e -> e.getCount() >= majority)
                                     .map(e -> e.getElement())
                                     .findFirst()
                                     .orElse(null);
            if (members != null) {
                if (gateway.complete(new Bound(v, seeds.stream().map(sn -> new NoteWrapper(sn, digestAlgo)).toList(),
                                               cardinality, joined, BloomFilter.from(g.getMembers())))) {
                    log.info("Gateway acquired: {} context: {} on: {}", v, this.context.getId(), node.getId());
                }
                return true;
            }
        }
        log.info("Gateway: {} majority not achieved: {} required: {} count: {} context: {} on: {}", v, cardinality,
                 majority, cards.size(), this.context.getId(), node.getId());
        return false;
    }

    private void validate(Redirect redirect, Duration duration, ScheduledExecutorService scheduler) {
        var v = Digest.from(redirect.getView());
        var successors = redirect.getSuccessorsList()
                                 .stream()
                                 .map(sn -> new NoteWrapper(sn.getNote(), digestAlgo))
                                 .map(nw -> view.new Participant(
                                                                 nw))
                                 .collect(Collectors.toList());
        log.info("Validating for: {} context: {} successors: {} on: {}", v, this.context.getId(), successors.size(),
                 node.getId());
        var validate = new CompletableFuture<Validations>();
        validate.whenComplete(join(redirect.getBootstrap(), v, redirect.getRings(), successors, duration, scheduler,
                                   redirect.getCardinality()));

        var revalidate = new AtomicReference<Runnable>();
        var retries = new AtomicInteger();

        Map<Member, Validation_> validations = new HashMap<>();

        log.info("Rebalancing to cardinality: {} (validation) for: {} context: {} on: {}", context.totalCount(), v,
                 context.getId(), node.getId());
        this.context.rebalance(redirect.getCardinality());
        node.nextNote(v);

        final var validating = new SliceIterator<>("Validation", node, successors, approaches, exec);
        var majority = redirect.getBootstrap() ? 1 : Context.minimalQuorum(redirect.getRings(), this.context.getBias());
        credentials(redirect.getNonce()).whenComplete((credentials, t) -> {
            revalidate.set(() -> {
                validating.iterate((link, m) -> {
                    log.debug("Validating from: {} contacting: {} on: {}", v, link.getMember().getId(), node.getId());
                    return link.register(credentials, params.seedingTimeout());
                }, (futureSailor, link, m) -> completeValidation((Participant) m, majority, v, validate, futureSailor,
                                                                 validations),
                                   () -> {
                                       if (retries.get() < params.joinRetries()) {
                                           log.debug("Failed to validate with view: {} retry: {} out of: {} on: {}", v,
                                                     retries.incrementAndGet(), params.joinRetries(), node.getId());
                                           validations.clear();
                                           scheduler.schedule(exec(() -> revalidate.get().run()),
                                                              Entropy.nextBitsStreamLong(params.retryDelay().toNanos()),
                                                              TimeUnit.NANOSECONDS);
                                       } else {
                                           log.error("Failed to validate with view: {} cannot obtain majority on: {}",
                                                     view, node.getId());
                                           view.stop();
                                       }
                                   }, scheduler, params.retryDelay());
            });
            revalidate.get().run();
        });
    }

    private boolean validate(Validation_ validation, Member from) {
        return true;
    }
}
