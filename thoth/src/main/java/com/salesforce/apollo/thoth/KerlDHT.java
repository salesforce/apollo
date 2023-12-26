/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Ordering;
import com.google.protobuf.Empty;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.RingCommunications;
import com.salesforce.apollo.ring.RingIterator;
import com.salesforce.apollo.stereotomy.DelegatedKERL;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.db.UniKERLDirectPooled;
import com.salesforce.apollo.stereotomy.db.UniKERLDirectPooled.ClosableKERL;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.proto.*;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLAdapter;
import com.salesforce.apollo.stereotomy.services.grpc.proto.KeyStates;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import com.salesforce.apollo.thoth.LoggingOutputStream.LogLevel;
import com.salesforce.apollo.thoth.grpc.dht.DhtClient;
import com.salesforce.apollo.thoth.grpc.dht.DhtServer;
import com.salesforce.apollo.thoth.grpc.dht.DhtService;
import com.salesforce.apollo.thoth.grpc.reconciliation.Reconciliation;
import com.salesforce.apollo.thoth.grpc.reconciliation.ReconciliationClient;
import com.salesforce.apollo.thoth.grpc.reconciliation.ReconciliationServer;
import com.salesforce.apollo.thoth.grpc.reconciliation.ReconciliationService;
import com.salesforce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.thoth.proto.Update;
import com.salesforce.apollo.thoth.proto.Updating;
import com.salesforce.apollo.utils.Entropy;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.Scope.Attr;
import liquibase.database.core.H2Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.ui.ConsoleUIService;
import liquibase.ui.UIService;
import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.digestOf;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.thoth.schema.Tables.IDENTIFIER_LOCATION_HASH;

/**
 * KerlDHT provides the replicated state store for KERLs
 *
 * @author hal.hildebrand
 */
public class KerlDHT implements ProtoKERLService {
    private final static Logger                                                      log            = LoggerFactory.getLogger(
    KerlDHT.class);
    private final static Logger                                                      reconcileLog   = LoggerFactory.getLogger(
    KerlSpace.class);
    private final        Ani                                                         ani;
    private final        CachingKERL                                                 cache;
    private final        JdbcConnectionPool                                          connectionPool;
    private final        Context<Member>                                             context;
    private final        CommonCommunications<DhtService, ProtoKERLService>          dhtComms;
    private final        double                                                      fpr;
    private final        Duration                                                    operationsFrequency;
    private final        CachingKERL                                                 kerl;
    private final        UniKERLDirectPooled                                         kerlPool;
    private final        KerlSpace                                                   kerlSpace;
    private final        SigningMember                                               member;
    private final        RingCommunications<Member, ReconciliationService>           reconcile;
    private final        CommonCommunications<ReconciliationService, Reconciliation> reconcileComms;
    private final        Reconcile                                                   reconciliation = new Reconcile();
    private final        ScheduledExecutorService                                    scheduler;
    private final        Service                                                     service        = new Service();
    private final        AtomicBoolean                                               started        = new AtomicBoolean();
    private final        TemporalAmount                                              operationTimeout;

    public KerlDHT(Duration operationsFrequency, Context<? extends Member> context, SigningMember member,
                   BiFunction<KerlDHT, KERL, KERL> wrap, JdbcConnectionPool connectionPool,
                   DigestAlgorithm digestAlgorithm, Router communications, TemporalAmount operationTimeout,
                   double falsePositiveRate, StereotomyMetrics metrics) {
        @SuppressWarnings("unchecked")
        final var casting = (Context<Member>) context;
        this.context = casting;
        this.member = member;
        this.operationTimeout = operationTimeout;
        this.fpr = falsePositiveRate;
        this.operationsFrequency = operationsFrequency;
        this.scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        this.cache = new CachingKERL(f -> {
            try {
                return f.apply(new KERLAdapter(this, digestAlgorithm()));
            } catch (Throwable t) {
                log.error("error applying cache", t);
                return null;
            }
        });
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
        this.reconcile = new RingCommunications<>(this.context, member, reconcileComms);
        this.kerlSpace = new KerlSpace(connectionPool);

        initializeSchema();
        kerl = new CachingKERL(f -> {
            try (var k = kerlPool.create()) {
                return f.apply(wrap.apply(this, wrap(k)));
            } catch (Throwable e) {
                log.error("Cannot apply kerl", e);
                return null;
            }
        });
        this.ani = new Ani(member.getId(), asKERL());
    }

    public KerlDHT(Duration operationsFrequency, Context<? extends Member> context, SigningMember member,
                   JdbcConnectionPool connectionPool, DigestAlgorithm digestAlgorithm, Router communications,
                   TemporalAmount operationTimeout, double falsePositiveRate, StereotomyMetrics metrics) {
        this(operationsFrequency, context, member, (t, k) -> k, connectionPool, digestAlgorithm, communications,
             operationTimeout, falsePositiveRate, metrics);
    }

    public static void updateLocationHash(Identifier identifier, DigestAlgorithm digestAlgorithm, DSLContext dsl) {
        dsl.transaction(config -> {
            var context = DSL.using(config);
            var identBytes = identifier.toIdent().toByteArray();
            // Braindead, but correct
            var id = context.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)).fetchOne();
            if (id == null) {
                throw new IllegalStateException("Identifier: %s not found".formatted(identifier));
            }

            var hashed = digestAlgorithm.digest(identBytes);
            context.insertInto(IDENTIFIER_LOCATION_HASH, IDENTIFIER_LOCATION_HASH.IDENTIFIER,
                               IDENTIFIER_LOCATION_HASH.DIGEST)
                   .values(id.value1(), hashed.getBytes())
                   .onDuplicateKeyIgnore()
                   .execute();
        });
    }

    static <T> T completeIt(T result) {
        return result;
    }

    public KeyState_ append(AttachmentEvent event) {
        if (event == null) {
            return null;
        }
        log.info("Append event: {} on: {}", EventCoordinates.from(event.getCoordinates()), member.getId());
        Digest identifier = digestOf(event, digestAlgorithm());
        if (identifier == null) {
            return null;
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStates>();
        HashMultiset<KeyStates> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(identifier, null,
                                                                                              (link, r) -> link.append(
                                                                                              Collections.emptyList(),
                                                                                              Collections.singletonList(
                                                                                              event)), null,
                                                                                              (tally, futureSailor, destination) -> mutate(
                                                                                              gathered, futureSailor,
                                                                                              identifier, isTimedOut,
                                                                                              tally, destination,
                                                                                              "append events"),
                                                                                              t -> completeIt(result,
                                                                                                              gathered));
        try {
            List<KeyState_> s = result.get().getKeyStatesList();
            return s.isEmpty() ? null : s.getFirst();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public List<KeyState_> append(KERL_ kerl) {
        if (kerl.getEventsList().isEmpty()) {
            return completeIt(Collections.emptyList());
        }
        final var event = kerl.getEventsList().get(0);
        Digest identifier = digestOf(event, digestAlgorithm());
        if (identifier == null) {
            return completeIt(Collections.emptyList());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStates>();
        HashMultiset<KeyStates> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(identifier, null,
                                                                                              (link, r) -> link.append(
                                                                                              kerl), null,
                                                                                              (tally, futureSailor, destination) -> mutate(
                                                                                              gathered, futureSailor,
                                                                                              identifier, isTimedOut,
                                                                                              tally, destination,
                                                                                              "append kerl"),
                                                                                              t -> completeIt(result,
                                                                                                              gathered));
        try {
            return result.get().getKeyStatesList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    public KeyState_ append(KeyEvent_ event) {
        Digest identifier = digestOf(event, digestAlgorithm());
        if (identifier == null) {
            return null;
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStates>();
        HashMultiset<KeyStates> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(identifier, null,
                                                                                              (link, r) -> link.append(
                                                                                              Collections.singletonList(
                                                                                              event)), null,
                                                                                              (tally, futureSailor, destination) -> mutate(
                                                                                              gathered, futureSailor,
                                                                                              identifier, isTimedOut,
                                                                                              tally, destination,
                                                                                              "append events"),
                                                                                              t -> completeIt(result,
                                                                                                              gathered));
        try {
            var ks = result.get();
            return ks.getKeyStatesCount() == 0 ? KeyState_.getDefaultInstance() : ks.getKeyStatesList().get(0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public List<KeyState_> append(List<KeyEvent_> events) {
        if (events.isEmpty()) {
            return completeIt(Collections.emptyList());
        }
        List<KeyState_> states = new ArrayList<>();
        events.stream().map(e -> append(e)).forEach(ks -> {
            states.add(ks);
        });
        return states;
    }

    @Override
    public List<KeyState_> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
        if (events.isEmpty()) {
            return completeIt(Collections.emptyList());
        }
        List<KeyState_> states = new ArrayList<>();
        events.stream().map(e -> append(e)).forEach(ks -> {
            states.add(ks);
        });

        attachments.forEach(this::append);
        return states;
    }

    @Override
    public Empty appendAttachments(List<AttachmentEvent> events) {
        if (events.isEmpty()) {
            return completeIt(Empty.getDefaultInstance());
        }
        final var event = events.get(0);
        Digest identifier = digestAlgorithm().digest(event.getCoordinates().getIdentifier().toByteString());
        if (identifier == null) {
            return completeIt(Empty.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Empty>();
        HashMultiset<Empty> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(identifier, null,
                                                                                              (link, r) -> link.appendAttachments(
                                                                                              events), null,
                                                                                              (tally, futureSailor, destination) -> mutate(
                                                                                              gathered, futureSailor,
                                                                                              identifier, isTimedOut,
                                                                                              tally, destination,
                                                                                              "append attachments"),
                                                                                              t -> completeIt(result,
                                                                                                              gathered));
        return Empty.getDefaultInstance();
    }

    @Override
    public Empty appendValidations(Validations validations) {
        if (validations.getValidationsCount() == 0) {
            return completeIt(null);
        }
        Digest identifier = digestAlgorithm().digest(validations.getCoordinates().getIdentifier().toByteString());
        if (identifier == null) {
            return completeIt(null);
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Empty>();
        HashMultiset<Empty> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(identifier, null,
                                                                                              (link, r) -> link.appendValidations(
                                                                                              validations), null,
                                                                                              (tally, futureSailor, destination) -> mutate(
                                                                                              gathered, futureSailor,
                                                                                              identifier, isTimedOut,
                                                                                              tally, destination,
                                                                                              "append validations"),
                                                                                              t -> completeIt(result,
                                                                                                              gathered));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    public KERL asKERL() {
        return cache;
    }

    /**
     * Clear the caches of the receiver
     */
    public void clearCache() {
        cache.clear();
    }

    public DigestAlgorithm digestAlgorithm() {
        return kerlPool.getDigestAlgorithm();
    }

    public Ani getAni() {
        return ani;
    }

    @Override
    public Attachment getAttachment(EventCoords coordinates) {
        if (coordinates == null) {
            return completeIt(Attachment.getDefaultInstance());
        }
        Digest identifier = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (identifier == null) {
            return completeIt(Attachment.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Attachment>();
        HashMultiset<Attachment> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(identifier, null,
                                                                                              (link, r) -> link.getAttachment(
                                                                                              coordinates),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, identifier,
                                                                                              isTimedOut, destination,
                                                                                              "get attachment",
                                                                                              Attachment.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KERL_ getKERL(Ident identifier) {
        if (identifier == null) {
            return completeIt(KERL_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return completeIt(KERL_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KERL_>();
        HashMultiset<KERL_> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(digest, null,
                                                                                              (link, r) -> link.getKERL(
                                                                                              identifier),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, digest,
                                                                                              isTimedOut, destination,
                                                                                              "get kerl",
                                                                                              KERL_.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyEvent_ getKeyEvent(EventCoords coordinates) {
        log.trace("Get key event: {} on: {}", EventCoordinates.from(coordinates), member.getId());
        if (coordinates == null) {
            return completeIt(KeyEvent_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return completeIt(KeyEvent_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyEvent_>();
        HashMultiset<KeyEvent_> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(digest, null,
                                                                                              (link, r) -> link.getKeyEvent(
                                                                                              coordinates),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, digest,
                                                                                              isTimedOut, destination,
                                                                                              "get key event",
                                                                                              KeyEvent_.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyState_ getKeyState(EventCoords coordinates) {
        log.info("Get key state: {} on: {}", EventCoordinates.from(coordinates), member.getId());
        if (coordinates == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        HashMultiset<KeyState_> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(digest, null,
                                                                                              (link, r) -> link.getKeyState(
                                                                                              coordinates),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, digest,
                                                                                              isTimedOut, destination,
                                                                                              "get key state for coordinates",
                                                                                              KeyState_.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyState_ getKeyState(Ident identifier, long sequenceNumber) {
        if (log.isInfoEnabled()) {
            log.info("Get key state for: {} seq#: {} on: {}", Identifier.from(identifier),
                     ULong.valueOf(sequenceNumber), member.getId());
        }
        if (identifier == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        var identAndSeq = IdentAndSeq.newBuilder().setIdentifier(identifier).setSequenceNumber(sequenceNumber).build();
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        HashMultiset<KeyState_> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).noDuplicates()
                                                                                     .iterate(digest, null,
                                                                                              (link, r) -> link.getKeyState(
                                                                                              identAndSeq),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, digest,
                                                                                              isTimedOut, destination,
                                                                                              "get key state for coordinates",
                                                                                              KeyState_.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyState_ getKeyState(Ident identifier) {
        log.info("Get key state: {} on: {}", Identifier.from(identifier), member.getId());
        if (identifier == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return completeIt(KeyState_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        HashMultiset<KeyState_> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).iterate(digest, null,
                                                                                              (link, r) -> link.getKeyState(
                                                                                              identifier),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, digest,
                                                                                              isTimedOut, destination,
                                                                                              "get current key state",
                                                                                              KeyState_.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coordinates) {
        log.info("Get key state with attachements: {} on: {}", EventCoordinates.from(coordinates), member.getId());
        if (coordinates == null) {
            return completeIt(KeyStateWithAttachments_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return completeIt(KeyStateWithAttachments_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStateWithAttachments_>();
        HashMultiset<KeyStateWithAttachments_> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).iterate(digest, null,
                                                                                              (link, r) -> link.getKeyStateWithAttachments(
                                                                                              coordinates),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, digest,
                                                                                              isTimedOut, destination,
                                                                                              "get key state with attachments",
                                                                                              KeyStateWithAttachments_.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
        log.info("Get key state with endorsements and validations: {} on: {}", EventCoordinates.from(coordinates),
                 member.getId());
        if (coordinates == null) {
            return completeIt(KeyStateWithEndorsementsAndValidations_.getDefaultInstance());
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return completeIt(KeyStateWithEndorsementsAndValidations_.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStateWithEndorsementsAndValidations_>();
        HashMultiset<KeyStateWithEndorsementsAndValidations_> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).iterate(digest, null,
                                                                                              (link, r) -> link.getKeyStateWithEndorsementsAndValidations(
                                                                                              coordinates),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, digest,
                                                                                              isTimedOut, destination,
                                                                                              "get key state with endorsements",
                                                                                              KeyStateWithEndorsementsAndValidations_.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public Validations getValidations(EventCoords coordinates) {
        log.info("Get validations: {} on: {}", EventCoordinates.from(coordinates), member.getId());
        if (coordinates == null) {
            return completeIt(Validations.getDefaultInstance());
        }
        Digest identifier = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (identifier == null) {
            return completeIt(Validations.getDefaultInstance());
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Validations>();
        HashMultiset<Validations> gathered = HashMultiset.create();
        new RingIterator<>(operationsFrequency, context, member, scheduler, dhtComms).iterate(identifier, null,
                                                                                              (link, r) -> link.getValidations(
                                                                                              coordinates),
                                                                                              () -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)),
                                                                                              (tally, futureSailor, destination) -> read(
                                                                                              result, gathered, tally,
                                                                                              futureSailor, identifier,
                                                                                              isTimedOut, destination,
                                                                                              "get validations",
                                                                                              Validations.getDefaultInstance()),
                                                                                              t -> failedMajority(
                                                                                              result,
                                                                                              maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    public <T> Entry<T> max(HashMultiset<T> gathered) {
        return gathered.entrySet().stream().max(Ordering.natural().onResultOf(Multiset.Entry::getCount)).orElse(null);
    }

    public int maxCount(HashMultiset<?> gathered) {
        final var max = gathered.entrySet().stream().max(Ordering.natural().onResultOf(Multiset.Entry::getCount));
        return max.isEmpty() ? 0 : max.get().getCount();
    }

    public void start(Duration duration) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        dhtComms.register(context.getId(), service);
        reconcileComms.register(context.getId(), reconciliation);
        reconcile(scheduler, duration);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        dhtComms.deregister(context.getId());
        reconcileComms.deregister(context.getId());
    }

    private <T> T complete(Function<ProtoKERLAdapter, T> func) {
        try {
            return func.apply(new ProtoKERLAdapter(kerl));
        } catch (Throwable t) {
            log.error("Error completing", t);
            return null;
        }
    }

    private <T> void completeIt(CompletableFuture<T> result, HashMultiset<T> gathered) {
        var max = gathered.entrySet()
                          .stream()
                          .max(Ordering.natural().onResultOf(Multiset.Entry::getCount))
                          .orElse(null);
        if (max != null) {
            if (max.getCount() >= context.majority()) {
                try {
                    result.complete(max.getElement());
                } catch (Throwable t) {
                    log.error("Unable to complete it", t);
                }
                return;
            }
        }
        result.completeExceptionally(new CompletionException(
        "Unable to achieve majority, max: " + (max == null ? 0 : max.getCount()) + " required: " + context.majority()
        + " on: " + member.getId()));
    }

    private boolean failedMajority(CompletableFuture<?> result, int maxAgree) {
        log.error("Unable to achieve majority read, max: {} required: {} on: {}", maxAgree, context.majority(),
                  member.getId());
        return result.completeExceptionally(new CompletionException(
        "Unable to achieve majority read, max: " + maxAgree + " required: " + context.majority() + " on: "
        + member.getId()));
    }

    private void initializeSchema() {
        ConsoleUIService service = (ConsoleUIService) Scope.getCurrentScope().get(Attr.ui, UIService.class);
        service.setOutputStream(
        new PrintStream(new LoggingOutputStream(LoggerFactory.getLogger("liquibase"), LogLevel.INFO)));
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

    private <T> boolean mutate(HashMultiset<T> gathered, Optional<T> futureSailor, Digest identifier,
                               Supplier<Boolean> isTimedOut, AtomicInteger tally,
                               RingCommunications.Destination<Member, DhtService> destination, String action) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        T content = futureSailor.get();
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

    private <T> boolean read(CompletableFuture<T> result, HashMultiset<T> gathered, AtomicInteger tally,
                             Optional<T> futureSailor, Digest identifier, Supplier<Boolean> isTimedOut,
                             RingCommunications.Destination<Member, DhtService> destination, String action, T empty) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        T content = futureSailor.get();
        if (content != null) {
            log.trace("{}: {} from: {}  on: {}", action, identifier, destination.member().getId(), member.getId());
            gathered.add(content);
            var max = max(gathered);
            if (max != null) {
                tally.set(max.getCount());
                // If there is only one active member in our context, it's us.
                final var majority = tally.get() >= (context.activeCount() == 1 ? 1 : context.majority());
                if (majority) {
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

    private void reconcile(Optional<Update> result,
                           RingCommunications.Destination<Member, ReconciliationService> destination,
                           ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        if (!result.isEmpty()) {
            try {
                Update update = result.get();
                if (update.getEventsCount() > 0) {
                    reconcileLog.trace("Received: {} events in interval reconciliation from: {} on: {}",
                                       update.getEventsCount(), destination.member().getId(), member.getId());
                    kerlSpace.update(update.getEventsList(), kerl);
                }
            } catch (NoSuchElementException e) {
                reconcileLog.debug("null interval reconciliation with {} : {} on: {}", destination.member().getId(),
                                   member.getId(), e.getCause());
            }
        }
        if (started.get()) {
            scheduler.schedule(() -> reconcile(scheduler, duration), duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private Update reconcile(ReconciliationService link, Integer ring) {
        if (member.equals(link.getMember())) {
            return null;
        }
        CombinedIntervals keyIntervals = keyIntervals();
        reconcileLog.trace("Interval reconciliation on ring: {} with: {} on: {} intervals: {}", ring,
                           link.getMember().getId(), member.getId(), keyIntervals);
        return link.reconcile(Intervals.newBuilder()
                                       .setRing(ring)
                                       .addAllIntervals(keyIntervals.toIntervals())
                                       .setHave(kerlSpace.populate(Entropy.nextBitsStreamLong(), keyIntervals, fpr))
                                       .build());
    }

    private void reconcile(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        Thread.ofVirtual()
              .start(() -> reconcile.execute((link, ring) -> reconcile(link, ring),
                                             (futureSailor, destination) -> reconcile(futureSailor, destination,
                                                                                      scheduler, duration)));

    }

    private void updateLocationHash(Identifier identifier) {
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            updateLocationHash(identifier, kerl.getDigestAlgorithm(), dsl);
        } catch (SQLException e) {
            log.error("Cannot update location hash for: {} on: {}", identifier, member.getId());
            throw new IllegalStateException(
            "Cannot update location hash S for: %s on: %s".formatted(identifier, member.getId()));
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
        return successor.equals(member);
    }

    private DelegatedKERL wrap(ClosableKERL k) {
        return new DelegatedKERL(k) {

            @Override
            public KeyState append(KeyEvent event) {
                KeyState ks = super.append(event);
                if (ks != null) {
                    updateLocationHash(ks.getCoordinates().getIdentifier());
                }
                return ks;
            }

            @Override
            public List<KeyState> append(KeyEvent... events) {
                List<KeyState> lks = super.append(events);
                if (lks.size() > 0) {
                    updateLocationHash(lks.get(0).getCoordinates().getIdentifier());
                }
                return lks;
            }

            @Override
            public List<KeyState> append(List<KeyEvent> events,
                                         List<com.salesforce.apollo.stereotomy.event.AttachmentEvent> attachments) {
                List<KeyState> lks = super.append(events, attachments);
                if (lks.size() > 0) {
                    updateLocationHash(lks.get(0).getCoordinates().getIdentifier());
                }
                return lks;
            }
        };
    }

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
                reconcileLog.trace("Invalid reconcile from: {} ring: {} on: {}", from, ring, member.getId());
                return Update.getDefaultInstance();
            }
            reconcileLog.trace("Reconcile from: {} ring: {} on: {}", from, ring, member.getId());
            try (var k = kerlPool.create()) {
                final var builder = KerlDHT.this.kerlSpace.reconcile(intervals, k);
                CombinedIntervals keyIntervals = keyIntervals();
                builder.addAllIntervals(keyIntervals.toIntervals())
                       .setHave(kerlSpace.populate(Entropy.nextBitsStreamLong(), keyIntervals, fpr));
                if (builder.getEventsCount() > 0) {
                    reconcileLog.trace("Reconcile for: {} ring: {} count: {} on: {}", from, ring,
                                       builder.getEventsCount(), member.getId());
                }
                return builder.build();
            } catch (IOException | SQLException e) {
                throw new IllegalStateException("Cannot acquire KERL", e);
            }
        }

        @Override
        public void update(Updating update, Digest from) {
            var ring = update.getRing();
            if (!valid(from, ring)) {
                return;
            }
            KerlDHT.this.kerlSpace.update(update.getEventsList(), kerl);
        }
    }

    private class Service implements ProtoKERLService {

        @Override
        public List<KeyState_> append(KERL_ kerl_) {
            log.info("appending kerl on: {}", member.getId());
            return complete(k -> k.append(kerl_));
        }

        @Override
        public List<KeyState_> append(List<KeyEvent_> events) {
            log.info("appending events on: {}", member.getId());
            return complete(k -> k.append(events));
        }

        @Override
        public List<KeyState_> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
            log.info("appending events and attachments on: {}", member.getId());
            return complete(k -> k.append(events, attachments));
        }

        @Override
        public Empty appendAttachments(List<AttachmentEvent> attachments) {
            log.info("append attachments on: {}", member.getId());
            return complete(k -> k.appendAttachments(attachments));
        }

        @Override
        public Empty appendValidations(Validations validations) {
            log.info("append validations on: {}", member.getId());
            return complete(k -> k.appendValidations(validations));
        }

        @Override
        public Attachment getAttachment(EventCoords coordinates) {
            log.trace("get attachments for coordinates on: {}", member.getId());
            return complete(k -> k.getAttachment(coordinates));
        }

        @Override
        public KERL_ getKERL(Ident identifier) {
            log.trace("get kerl for identifier on: {}", member.getId());
            return complete(k -> k.getKERL(identifier));
        }

        @Override
        public KeyEvent_ getKeyEvent(EventCoords coordinates) {
            log.trace("get key event for coordinates on: {}", member.getId());
            final Function<ProtoKERLAdapter, KeyEvent_> func = k -> {
                return k.getKeyEvent(coordinates);
            };
            return complete(func);
        }

        @Override
        public KeyState_ getKeyState(EventCoords coordinates) {
            log.trace("get key state for coordinates on: {}", member.getId());
            return complete(k -> k.getKeyState(coordinates));
        }

        @Override
        public KeyState_ getKeyState(Ident identifier, long sequenceNumber) {
            return null;
        }

        @Override
        public KeyState_ getKeyState(Ident identifier) {
            log.trace("get key state for identifier on: {}", member.getId());
            return complete(k -> k.getKeyState(identifier));
        }

        @Override
        public KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coords) {
            log.trace("get key state with attachments for coordinates on: {}", member.getId());
            return complete(k -> k.getKeyStateWithAttachments(coords));
        }

        @Override
        public KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(
        EventCoords coordinates) {
            log.trace("get key state with endorsements and attachments for coordinates on: {}", member.getId());
            return complete(k -> {
                final var fs = new CompletableFuture<KeyStateWithEndorsementsAndValidations_>();
                KeyStateWithAttachments_ ksa = k.getKeyStateWithAttachments(coordinates);
                var validations = complete(ks -> ks.getValidations(coordinates));

                return ksa == null ? KeyStateWithEndorsementsAndValidations_.getDefaultInstance()
                                   : KeyStateWithEndorsementsAndValidations_.newBuilder()
                                                                            .setState(ksa.getState())
                                                                            .putAllEndorsements(
                                                                            ksa.getAttachment().getEndorsementsMap())
                                                                            .addAllValidations(
                                                                            validations.getValidationsList())
                                                                            .build();
            });
        }

        @Override
        public Validations getValidations(EventCoords coordinates) {
            log.trace("get validations for coordinates on: {}", member.getId());
            return complete(k -> k.getValidations(coordinates));
        }
    }
}
