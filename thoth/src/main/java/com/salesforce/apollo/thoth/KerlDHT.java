/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.digestOf;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.thoth.schema.Tables.RING_DIGESTS;

import java.io.PrintStream;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Ordering;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithAttachments_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithEndorsementsAndValidations_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyStates;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesfoce.apollo.thoth.proto.Update;
import com.salesfoce.apollo.thoth.proto.Updating;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.RingCommunications;
import com.salesforce.apollo.ring.RingCommunications.Destination;
import com.salesforce.apollo.ring.RingIterator;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.db.UniKERLDirectPooled;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLAdapter;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import com.salesforce.apollo.thoth.grpc.dht.DhtClient;
import com.salesforce.apollo.thoth.grpc.dht.DhtServer;
import com.salesforce.apollo.thoth.grpc.dht.DhtService;
import com.salesforce.apollo.thoth.grpc.reconciliation.Reconciliation;
import com.salesforce.apollo.thoth.grpc.reconciliation.ReconciliationClient;
import com.salesforce.apollo.thoth.grpc.reconciliation.ReconciliationServer;
import com.salesforce.apollo.thoth.grpc.reconciliation.ReconciliationService;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.LoggingOutputStream;
import com.salesforce.apollo.utils.LoggingOutputStream.LogLevel;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.Scope.Attr;
import liquibase.database.core.H2Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.ui.ConsoleUIService;
import liquibase.ui.UIService;

/**
 * KerlDHT provides the replicated state store for KERLs
 *
 * @author hal.hildebrand
 *
 */
public class KerlDHT implements ProtoKERLService {
    public static class CompletionException extends Exception {

        private static final long serialVersionUID = 1L;

        public CompletionException(String message) {
            super(message);
        }
    }

    private class Reconcile implements Reconciliation {

        @Override
        public Update reconcile(Intervals intervals, Digest from) {
            var ring = intervals.getRing();
            if (!valid(from, ring)) {
                return Update.getDefaultInstance();
            }

            return KerlDHT.this.kerlSpace.reconcile(intervals);
        }

        @Override
        public void update(Updating update, Digest from) {
            var ring = update.getRing();
            if (!valid(from, ring)) {
                return;
            }
            KerlDHT.this.kerlSpace.update(update.getEventsList());
        }
    }

    private class Service implements ProtoKERLService {

        @Override
        public CompletableFuture<List<KeyState_>> append(KERL_ kerl_) {
            log.info("appending kerl on: {}", member.getId());
            return complete(k -> k.append(kerl_).thenApply(lks -> {
                if (lks.size() > 0) {
                    updateRings(lks.get(0).getCoordinates().getIdentifier());
                }
                return lks;
            }));
        }

        @Override
        public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events) {
            log.info("appending events on: {}", member.getId());
            return complete(k -> k.append(events).thenApply(lks -> {
                if (lks.size() > 0) {
                    updateRings(lks.get(0).getCoordinates().getIdentifier());
                }
                return lks;
            }));
        }

        @Override
        public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
            log.info("appending events and attachments on: {}", member.getId());
            return complete(k -> k.append(events, attachments).thenApply(lks -> {
                if (lks.size() > 0) {
                    updateRings(lks.get(0).getCoordinates().getIdentifier());
                }
                return lks;
            }));
        }

        @Override
        public CompletableFuture<Empty> appendAttachments(List<AttachmentEvent> attachments) {
            log.info("append attachments on: {}", member.getId());
            return complete(k -> k.appendAttachments(attachments));
        }

        @Override
        public CompletableFuture<Empty> appendValidations(Validations validations) {
            log.info("append validations on: {}", member.getId());
            return complete(k -> k.appendValidations(validations));
        }

        @Override
        public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
            log.trace("get attachments for coordinates on: {}", member.getId());
            return complete(k -> k.getAttachment(coordinates));
        }

        @Override
        public CompletableFuture<KERL_> getKERL(Ident identifier) {
            log.trace("get kerl for identifier on: {}", member.getId());
            return complete(k -> k.getKERL(identifier));
        }

        @Override
        public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
            log.trace("get key event for coordinates on: {}", member.getId());
            final Function<ProtoKERLAdapter, CompletableFuture<KeyEvent_>> func = k -> {
                return k.getKeyEvent(coordinates);
            };
            return complete(func);
        }

        @Override
        public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
            log.trace("get key state for coordinates on: {}", member.getId());
            return complete(k -> k.getKeyState(coordinates));
        }

        @Override
        public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
            log.trace("get key state for identifier on: {}", member.getId());
            return complete(k -> k.getKeyState(identifier));
        }

        @Override
        public CompletableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coords) {
            log.trace("get key state with attachments for coordinates on: {}", member.getId());
            return complete(k -> k.getKeyStateWithAttachments(coords));
        }

        @Override
        public CompletableFuture<KeyStateWithEndorsementsAndValidations_> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
            log.trace("get key state with endorsements and attachments for coordinates on: {}", member.getId());
            return complete(k -> {
                final var fs = new CompletableFuture<KeyStateWithEndorsementsAndValidations_>();
                k.getKeyStateWithAttachments(coordinates)
                 .thenAcceptBoth(complete(ke -> ke.getValidations(coordinates)), (ksa, validations) -> {
                     var result = ksa == null ? KeyStateWithEndorsementsAndValidations_.getDefaultInstance()
                                              : KeyStateWithEndorsementsAndValidations_.newBuilder()
                                                                                       .setState(ksa.getState())
                                                                                       .putAllEndorsements(ksa.getAttachment()
                                                                                                              .getEndorsementsMap())
                                                                                       .addAllValidations(validations.getValidationsList())
                                                                                       .build();
                     fs.complete(result);
                 })
                 .exceptionally(t -> {
                     fs.completeExceptionally(t);
                     return null;
                 });
                return fs;
            });
        }

        @Override
        public CompletableFuture<Validations> getValidations(EventCoords coordinates) {
            log.trace("get validations for coordinates on: {}", member.getId());
            return complete(k -> k.getValidations(coordinates));
        }
    }

    private final static Logger log = LoggerFactory.getLogger(KerlDHT.class);

    public static <T> CompletableFuture<T> completeExceptionally(Throwable t) {
        var fs = new CompletableFuture<T>();
        fs.completeExceptionally(t);
        return fs;
    }

    static <T> CompletableFuture<T> completeIt(T result) {
        var fs = new CompletableFuture<T>();
        fs.complete(result);
        return fs;
    }

    private final Ani                                                         ani;
    private final KERL                                                        cache;
    private final JdbcConnectionPool                                          connectionPool;
    private final Context<Member>                                             context;
    private final CommonCommunications<DhtService, ProtoKERLService>          dhtComms;
    private final Executor                                                    executor;
    private final double                                                      fpr;
    private final Duration                                                    frequency;
    private final CachingKERL                                                 kerl;
    private final UniKERLDirectPooled                                         kerlPool;
    private final KerlSpace                                                   kerlSpace;
    private final SigningMember                                               member;
    private final RingCommunications<Member, ReconciliationService>           reconcile;
    private final CommonCommunications<ReconciliationService, Reconciliation> reconcileComms;
    private final Reconcile                                                   reconciliation = new Reconcile();
    private final ScheduledExecutorService                                    scheduler;
    private final Service                                                     service        = new Service();
    private final AtomicBoolean                                               started        = new AtomicBoolean();
    private final TemporalAmount                                              timeout;

    public KerlDHT(Duration frequency, Context<? extends Member> context, SigningMember member,
                   BiFunction<KerlDHT, KERL, KERL> wrap, JdbcConnectionPool connectionPool,
                   DigestAlgorithm digestAlgorithm, Router communications, Executor executor, TemporalAmount timeout,
                   ScheduledExecutorService scheduler, double falsePositiveRate, StereotomyMetrics metrics) {
        @SuppressWarnings("unchecked")
        final var casting = (Context<Member>) context;
        this.context = casting;
        this.member = member;
        this.timeout = timeout;
        this.fpr = falsePositiveRate;
        this.frequency = frequency;
        this.scheduler = scheduler;
        this.cache = new CachingKERL(f -> f.apply(new KERLAdapter(this, digestAlgorithm())));
        dhtComms = communications.create(member, context.getId(), service, service.getClass().getCanonicalName(),
                                         r -> new DhtServer(r, metrics), DhtClient.getCreate(metrics),
                                         DhtClient.getLocalLoopback(service, member));
        reconcileComms = communications.create(member, context.getId(), reconciliation,
                                               reconciliation.getClass().getCanonicalName(),
                                               r -> new ReconciliationServer(r,
                                                                             communications.getClientIdentityProvider(),
                                                                             metrics),
                                               ReconciliationClient.getCreate(context.getId(), metrics),
                                               ReconciliationClient.getLocalLoopback(reconciliation, member));
        this.connectionPool = connectionPool;
        kerlPool = new UniKERLDirectPooled(connectionPool, digestAlgorithm);
        this.executor = executor;
        this.reconcile = new RingCommunications<>(this.context, member, reconcileComms, executor);
        this.kerlSpace = new KerlSpace(connectionPool);

        initializeSchema();
        kerl = new CachingKERL(f -> {
            try (var k = kerlPool.create()) {
                return f.apply(wrap.apply(this, k));
            } catch (Throwable e) {
                return completeExceptionally(e);
            }
        });
        this.ani = new Ani(member.getId(), asKERL());
    }

    public KerlDHT(Duration frequency, Context<? extends Member> context, SigningMember member,
                   JdbcConnectionPool connectionPool, DigestAlgorithm digestAlgorithm, Router communications,
                   Executor executor, TemporalAmount timeout, ScheduledExecutorService scheduler,
                   double falsePositiveRate, StereotomyMetrics metrics) {
        this(frequency, context, member, (t, k) -> k, connectionPool, digestAlgorithm, communications, executor,
             timeout, scheduler, falsePositiveRate, metrics);
    }

    public CompletableFuture<KeyState_> append(AttachmentEvent event) {
        if (event == null) {
            return complete(null);
        }
        log.info("Append event: {} on: {}", EventCoordinates.from(event.getCoordinates()), member.getId());
        Digest identifier = digestOf(event, digestAlgorithm());
        if (identifier == null) {
            return complete(null);
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStates>();
        HashMultiset<KeyStates> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(identifier, null,
                                             (link, r) -> link.append(Collections.emptyList(),
                                                                      Collections.singletonList(event)),
                                             null,
                                             (tally, futureSailor, destination) -> mutate(gathered, futureSailor,
                                                                                          identifier, isTimedOut, tally,
                                                                                          destination, "append events"),
                                             t -> completeIt(result, gathered));
        return result.thenApply(ks -> KeyState_.getDefaultInstance());
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(KERL_ kerl) {
        if (kerl.getEventsList().isEmpty()) {
            return completeIt(Collections.emptyList());
        }
        final var event = kerl.getEventsList().get(0);
        Digest identifier = digestOf(event, digestAlgorithm());
        if (identifier == null) {
            return completeIt(Collections.emptyList());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStates>();
        HashMultiset<KeyStates> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(identifier, null, (link, r) -> link.append(kerl), null,
                                             (tally, futureSailor, destination) -> mutate(gathered, futureSailor,
                                                                                          identifier, isTimedOut, tally,
                                                                                          destination, "append kerl"),
                                             t -> completeIt(result, gathered));
        return result.thenApply(ks -> ks.getKeyStatesList());
    }

    public CompletableFuture<KeyState_> append(KeyEvent_ event) {
        Digest identifier = digestOf(event, digestAlgorithm());
        if (identifier == null) {
            return complete(null);
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStates>();
        HashMultiset<KeyStates> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(identifier, null,
                                             (link, r) -> link.append(Collections.singletonList(event)), null,
                                             (tally, futureSailor, destination) -> mutate(gathered, futureSailor,
                                                                                          identifier, isTimedOut, tally,
                                                                                          destination, "append events"),
                                             t -> completeIt(result, gathered));
        return result.thenApply(ks -> ks.getKeyStatesCount() == 0 ? KeyState_.getDefaultInstance()
                                                                  : ks.getKeyStatesList().get(0));
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events) {
        if (events.isEmpty()) {
            return completeIt(Collections.emptyList());
        }
        List<KeyState_> states = new ArrayList<>();
        var futures = events.stream().map(e -> append(e).thenApply(ks -> {
            states.add(ks);
            return ks;
        })).toList();
        return futures.stream().reduce((a, b) -> a.thenCompose(ks -> b)).get().thenApply(ks -> states);
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
        if (events.isEmpty()) {
            return completeIt(Collections.emptyList());
        }
        List<KeyState_> states = new ArrayList<>();
        var futures = events.stream().map(e -> append(e).thenApply(ks -> {
            states.add(ks);
            return ks;
        }));

        return Streams.concat(futures, attachments.stream().map(a -> append(a)))
                      .reduce((a, b) -> a.thenCompose(ks -> b))
                      .get()
                      .thenApply(ks -> states);
    }

    @Override
    public CompletableFuture<Empty> appendAttachments(List<AttachmentEvent> events) {
        if (events.isEmpty()) {
            return completeIt(Empty.getDefaultInstance());
        }
        final var event = events.get(0);
        Digest identifier = digestAlgorithm().digest(event.getCoordinates().getIdentifier().toByteString());
        if (identifier == null) {
            return completeIt(Empty.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Empty>();
        HashMultiset<Empty> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(identifier, null, (link, r) -> link.appendAttachments(events), null,
                                             (tally, futureSailor,
                                              destination) -> mutate(gathered, futureSailor, identifier, isTimedOut,
                                                                     tally, destination, "append attachments"),
                                             t -> completeIt(result, gathered));
        return result;
    }

    @Override
    public CompletableFuture<Empty> appendValidations(Validations validations) {
        if (validations.getValidationsCount() == 0) {
            return completeIt(null);
        }
        Digest identifier = digestAlgorithm().digest(validations.getCoordinates().getIdentifier().toByteString());
        if (identifier == null) {
            return completeIt(null);
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Empty>();
        HashMultiset<Empty> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(identifier, null, (link, r) -> link.appendValidations(validations), null,
                                             (tally, futureSailor,
                                              destination) -> mutate(gathered, futureSailor, identifier, isTimedOut,
                                                                     tally, destination, "append validations"),
                                             t -> completeIt(result, gathered));
        return result;
    }

    public KERL asKERL() {
        return cache;
    }

    public DigestAlgorithm digestAlgorithm() {
        return kerlPool.getDigestAlgorithm();
    }

    public Ani getAni() {
        return ani;
    }

    @Override
    public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
        if (coordinates == null) {
            return completeIt(Attachment.getDefaultInstance());
        }
        Digest identifier = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (identifier == null) {
            return completeIt(Attachment.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Attachment>();
        HashMultiset<Attachment> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(identifier, null, (link, r) -> link.getAttachment(coordinates),
                                             () -> failedMajority(result, maxCount(gathered)),
                                             (tally, futureSailor,
                                              destination) -> read(result, gathered, tally, futureSailor, identifier,
                                                                   isTimedOut, destination, "get attachment",
                                                                   Attachment.getDefaultInstance()),
                                             t -> failedMajority(result, maxCount(gathered)));
        return result;
    }

    @Override
    public CompletableFuture<KERL_> getKERL(Ident identifier) {
        if (identifier == null) {
            return completeIt(KERL_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return completeIt(KERL_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KERL_>();
        HashMultiset<KERL_> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(digest, null, (link, r) -> link.getKERL(identifier),
                                             () -> failedMajority(result, maxCount(gathered)),
                                             (tally, futureSailor,
                                              destination) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, "get kerl",
                                                                   KERL_.getDefaultInstance()),
                                             t -> failedMajority(result, maxCount(gathered)));
        return result;
    }

    @Override
    public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        log.trace("Get key event: {} on: {}", EventCoordinates.from(coordinates), member.getId());
        if (coordinates == null) {
            return completeIt(KeyEvent_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return completeIt(KeyEvent_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyEvent_>();
        HashMultiset<KeyEvent_> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(digest, null, (link, r) -> link.getKeyEvent(coordinates),
                                             () -> failedMajority(result, maxCount(gathered)),
                                             (tally, futureSailor,
                                              destination) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, "get key event",
                                                                   KeyEvent_.getDefaultInstance()),
                                             t -> failedMajority(result, maxCount(gathered)));
        return result;
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
        log.info("Get key state: {} on: {}", EventCoordinates.from(coordinates), member.getId());
        if (coordinates == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        HashMultiset<KeyState_> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).noDuplicates()
                                    .iterate(digest, null, (link, r) -> link.getKeyState(coordinates),
                                             () -> failedMajority(result, maxCount(gathered)),
                                             (tally, futureSailor, destination) -> read(result, gathered, tally,
                                                                                        futureSailor, digest,
                                                                                        isTimedOut, destination,
                                                                                        "get key state for coordinates",
                                                                                        KeyState_.getDefaultInstance()),
                                             t -> failedMajority(result, maxCount(gathered)));
        return result;
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
        log.info("Get key state: {} on: {}", Identifier.from(identifier), member.getId());
        if (identifier == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        HashMultiset<KeyState_> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).iterate(digest, null, (link, r) -> link.getKeyState(identifier),
                                             () -> failedMajority(result, maxCount(gathered)),
                                             (tally, futureSailor,
                                              destination) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, "get current key state",
                                                                   KeyState_.getDefaultInstance()),
                                             t -> failedMajority(result, maxCount(gathered)));
        return result;
    }

    @Override
    public CompletableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coordinates) {
        log.info("Get key state with attachements: {} on: {}", EventCoordinates.from(coordinates), member.getId());
        if (coordinates == null) {
            return completeIt(KeyStateWithAttachments_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return completeIt(KeyStateWithAttachments_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStateWithAttachments_>();
        HashMultiset<KeyStateWithAttachments_> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).iterate(digest, null, (link, r) -> link.getKeyStateWithAttachments(coordinates),
                                             () -> failedMajority(result, maxCount(gathered)),
                                             (tally, futureSailor,
                                              destination) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination,
                                                                   "get key state with attachments",
                                                                   KeyStateWithAttachments_.getDefaultInstance()),
                                             t -> failedMajority(result, maxCount(gathered)));
        return result;
    }

    @Override
    public CompletableFuture<KeyStateWithEndorsementsAndValidations_> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
        log.info("Get key state with endorsements and validations: {} on: {}", EventCoordinates.from(coordinates),
                 member.getId());
        if (coordinates == null) {
            return completeIt(KeyStateWithEndorsementsAndValidations_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return completeIt(KeyStateWithEndorsementsAndValidations_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStateWithEndorsementsAndValidations_>();
        HashMultiset<KeyStateWithEndorsementsAndValidations_> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).iterate(digest, null,
                                             (link, r) -> link.getKeyStateWithEndorsementsAndValidations(coordinates),
                                             () -> failedMajority(result, maxCount(gathered)),
                                             (tally, futureSailor,
                                              destination) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination,
                                                                   "get key state with endorsements",
                                                                   KeyStateWithEndorsementsAndValidations_.getDefaultInstance()),
                                             t -> failedMajority(result, maxCount(gathered)));
        return result;
    }

    @Override
    public CompletableFuture<Validations> getValidations(EventCoords coordinates) {
        log.info("Get validations: {} on: {}", EventCoordinates.from(coordinates), member.getId());
        if (coordinates == null) {
            return completeIt(Validations.getDefaultInstance());
        }
        Digest identifier = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (identifier == null) {
            return completeIt(Validations.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Validations>();
        HashMultiset<Validations> gathered = HashMultiset.create();
        new RingIterator<>(frequency, context, member, scheduler, dhtComms,
                           executor).iterate(identifier, null, (link, r) -> link.getValidations(coordinates),
                                             () -> failedMajority(result, maxCount(gathered)),
                                             (tally, futureSailor,
                                              destination) -> read(result, gathered, tally, futureSailor, identifier,
                                                                   isTimedOut, destination, "get validations",
                                                                   Validations.getDefaultInstance()),
                                             t -> failedMajority(result, maxCount(gathered)));
        return result;
    }

    public BiConsumer<List<EventCoordinates>, List<Digest>> listener() {
        return (joining, leaving) -> {
            executor.execute(Utils.wrapped(() -> {
                kerlSpace.rebalance(joining, leaving);
            }, log));
        };
    }

    public <T> Entry<T> max(HashMultiset<T> gathered) {
        return gathered.entrySet().stream().max(Ordering.natural().onResultOf(Multiset.Entry::getCount)).orElse(null);
    }

    public int maxCount(HashMultiset<?> gathered) {
        final var max = gathered.entrySet().stream().max(Ordering.natural().onResultOf(Multiset.Entry::getCount));
        return max.isEmpty() ? 0 : max.get().getCount();
    }

    public void start(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        dhtComms.register(context.getId(), service);
        reconcileComms.register(context.getId(), reconciliation);
//        reconcile(scheduler, duration); TODO
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        dhtComms.deregister(context.getId());
        reconcileComms.deregister(context.getId());
    }

    private <T> CompletableFuture<T> complete(Function<ProtoKERLAdapter, CompletableFuture<T>> func) {
        return func.apply(new ProtoKERLAdapter(kerl));
    }

    private <T> void completeIt(CompletableFuture<T> result, HashMultiset<T> gathered) {
        var max = gathered.entrySet()
                          .stream()
                          .max(Ordering.natural().onResultOf(Multiset.Entry::getCount))
                          .orElse(null);
        if (max != null) {
            if (max.getCount() >= context.majority()) {
                result.complete(max.getElement());
                return;
            }
        }
        result.completeExceptionally(new CompletionException("Unable to achieve majority, max: "
        + (max == null ? 0 : max.getCount()) + " required: " + context.majority() + " on: " + member.getId()));
    }

    private boolean failedMajority(CompletableFuture<?> result, int maxAgree) {
        return result.completeExceptionally(new CompletionException("Unable to achieve majority read, max: " + maxAgree
        + " required: " + context.majority() + " on: " + member.getId()));
    }

    private void initializeSchema() {
        ConsoleUIService service = (ConsoleUIService) Scope.getCurrentScope().get(Attr.ui, UIService.class);
        service.setOutputStream(new PrintStream(new LoggingOutputStream(LoggerFactory.getLogger("liquibase"),
                                                                        LogLevel.INFO)));
        var database = new H2Database();
        try (var connection = connectionPool.getConnection()) {
            database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
            try (Liquibase liquibase = new Liquibase("/initialize-thoth.xml", new ClassLoaderResourceAccessor(),
                                                     database)) {
                liquibase.update((String) null);
            } catch (LiquibaseException e) {
                throw new IllegalStateException(e);
            }
        } catch (SQLException e1) {
            throw new IllegalStateException(e1);
        }
    }

    private CombinedIntervals keyIntervals() {
        List<KeyInterval> intervals = new ArrayList<>();
        for (int i = 0; i < context.getRingCount(); i++) {
            Ring<Member> ring = context.ring(i);
            Member predecessor = ring.predecessor(member);
            if (predecessor == null) {
                continue;
            }

            Digest begin = ring.hash(predecessor);
            Digest end = ring.hash(member);

            if (begin.compareTo(end) > 0) { // wrap around the origin of the ring
                intervals.add(new KeyInterval(end, digestAlgorithm().getLast()));
                intervals.add(new KeyInterval(digestAlgorithm().getOrigin(), begin));
            } else {
                intervals.add(new KeyInterval(begin, end));
            }
        }
        return new CombinedIntervals(intervals);
    }

    private <T> boolean mutate(HashMultiset<T> gathered, Optional<ListenableFuture<T>> futureSailor, Digest identifier,
                               Supplier<Boolean> isTimedOut, AtomicInteger tally,
                               Destination<Member, DhtService> destination, String action) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        T content = null;
        try {
            content = futureSailor.get().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) e.getCause();
                if (sre.getStatus() == Status.UNAVAILABLE) {
                    log.trace("Server unavailable action: {} for: {} from: {} on: {}", action, identifier,
                              destination.member().getId(), member.getId());
                } else {
                    log.trace("Server status: {} : {} action: {} for: {} from: {} on: {}", sre.getStatus().getCode(),
                              sre.getStatus().getDescription(), action, identifier, destination.member().getId(),
                              member.getId());
                }
            } else {
                log.trace("Error {}: {} from: {} on: {}", action, identifier, destination.member().getId(),
                          member.getId(), e.getCause());
            }
            return !isTimedOut.get();
        }
        if (content != null) {
            log.trace("{}: {} from: {}  on: {}", action, identifier, destination.member().getId(), member.getId());
            gathered.add(content);
            var max = gathered.entrySet()
                              .stream()
                              .max(Ordering.natural().onResultOf(Multiset.Entry::getCount))
                              .orElse(null);
            if (max != null) {
                tally.set(max.getCount());
            }
            return !isTimedOut.get();
        } else {
            log.debug("Failed {}: {} from: {}  on: {}", action, identifier, destination.member().getId(),
                      member.getId());
            return !isTimedOut.get();
        }
    }

    private Biff populate(CombinedIntervals keyIntervals) {
        List<Digest> digests = kerlSpace.populate(keyIntervals);
        var biff = new DigestBloomFilter(Entropy.nextBitsStreamLong(), digests.size(), fpr);
        return biff.toBff();
    }

    private <T> boolean read(CompletableFuture<T> result, HashMultiset<T> gathered, AtomicInteger tally,
                             Optional<ListenableFuture<T>> futureSailor, Digest identifier,
                             Supplier<Boolean> isTimedOut, Destination<Member, DhtService> destination, String action,
                             T empty) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        T content = null;
        try {
            content = futureSailor.get().get();
        } catch (InterruptedException e) {
            log.debug("Error {}: {} from: {} on: {}", action, identifier, destination.member(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) t;
                log.trace("Error {}: {} : {} on: {}", action, identifier, sre.getStatus(), destination.member().getId(),
                          member.getId());
                return !isTimedOut.get();
            } else {
                log.debug("Error {}: {} from: {} on: {}", action, identifier, destination.member(), member,
                          e.getCause());
                return !isTimedOut.get();
            }
        }
        if (content != null) {
            log.trace("{}: {} from: {}  on: {}", action, identifier, destination.member().getId(), member.getId());
            gathered.add(content);
            var max = max(gathered);
            if (max != null) {
                tally.set(max.getCount());
                if (max.getCount() > context.toleranceLevel()) {
                    result.complete(max.getElement());
                    log.debug("Majority: {} achieved: {}: {} on: {}", max.getCount(), action, identifier,
                              member.getId());
                    return false;
                }
            }
            return !isTimedOut.get();
        } else {
            log.debug("Failed {}: {} from: {}  on: {}", action, identifier, destination.member().getId(),
                      member.getId());
            return !isTimedOut.get();
        }
    }

    private void reconcile(Optional<ListenableFuture<Update>> futureSailor,
                           Destination<Member, ReconciliationService> destination, ScheduledExecutorService scheduler,
                           Duration duration) {
        if (!started.get() || futureSailor.isEmpty()) {
            return;
        }
        try {
            Update update = futureSailor.get().get();
            log.trace("Received: {} events in interval reconciliation from: {} on: {}", update.getEventsCount(),
                      destination.member().getId(), member.getId());
            kerlSpace.update(update.getEventsList());
        } catch (InterruptedException | ExecutionException e) {
            log.debug("Error in interval reconciliation with {} : {} on: {}", destination.member().getId(),
                      member.getId(), e.getCause());
        }
        if (started.get()) {
            scheduler.schedule(() -> reconcile(scheduler, duration), duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private ListenableFuture<Update> reconcile(ReconciliationService link, Integer ring) {
        CombinedIntervals keyIntervals = keyIntervals();
        log.trace("Interval reconciliation on ring: {} with: {} on: {} intervals: {}", ring, link.getMember(),
                  member.getId(), keyIntervals);
        return link.reconcile(Intervals.newBuilder()
                                       .setRing(ring)
                                       .addAllIntervals(keyIntervals.toIntervals())
                                       .setHave(populate(keyIntervals))
                                       .build());
    }

    private void reconcile(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        reconcile.execute((link, ring) -> reconcile(link, ring),
                          (futureSailor, destination) -> reconcile(futureSailor, destination, scheduler, duration));

    }

    private void updateRings(Ident identifier) {
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            dsl.transaction(create -> {
                var id = dsl.select(IDENTIFIER.ID)
                            .from(IDENTIFIER)
                            .where(IDENTIFIER.PREFIX.eq(identifier.toByteArray()))
                            .fetchOne();
                if (id == null) {
                    log.error("Identifier: {} not found on: {}", Identifier.from(identifier), member.getId());
                    throw new IllegalStateException("Identifier: %s not found on: %s".formatted(Identifier.from(identifier),
                                                                                                member.getId()));
                }

                var batch = dsl.batch(dsl.insertInto(RING_DIGESTS, RING_DIGESTS.IDENTIFIER, RING_DIGESTS.RING,
                                                     RING_DIGESTS.DIGEST)
                                         .values((Long) null, null, null)
                                         .onDuplicateKeyIgnore());
                var hashed = kerl.getDigestAlgorithm().digest(identifier.toByteString());
                for (var r = 0; r < context.getRingCount(); r++) {
                    batch.bind(id.value1(), r, context.hashFor(hashed, r).getBytes());
                }
                batch.execute();
            });
        } catch (SQLException e) {
            log.error("Cannot update ring hashes for: {} on: {}", Identifier.from(identifier), member.getId());
            throw new IllegalStateException("Cannot update ring hashes for: %s on: %s".formatted(Identifier.from(identifier),
                                                                                                 member.getId()));
        }
    }

    private boolean valid(Digest from, int ring) {
        if (ring >= context.getRingCount() || ring < 0) {
            log.warn("invalid ring {} from {} on: {}", ring, from, member.getId());
            return false;
        }
        Member fromMember = context.getMember(from);
        if (fromMember == null) {
            return false;
        }
        Member successor = context.ring(ring).successor(fromMember, m -> context.isActive(m.getId()));
        if (successor == null) {
            return false;
        }
        if (!successor.equals(member)) {
            return false;
        }
        return true;
    }
}
