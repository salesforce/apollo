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
import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.DelegatedContext;
import com.salesforce.apollo.context.StaticContext;
import com.salesforce.apollo.context.ViewChange;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.stereotomy.*;
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
import com.salesforce.apollo.utils.Utils;
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
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.digestOf;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.thoth.schema.Tables.IDENTIFIER_LOCATION_HASH;
import static com.salesforce.apollo.utils.Utils.b64;

/**
 * KerlDHT provides the replicated state store for KERLs
 *
 * @author hal.hildebrand
 */
public class KerlDHT implements ProtoKERLService {
    private final static Logger log          = LoggerFactory.getLogger(KerlDHT.class);
    private final static Logger reconcileLog = LoggerFactory.getLogger(KerlSpace.class);

    private final Ani                                                         ani;
    private final CachingKERL                                                 cache;
    private final JdbcConnectionPool                                          connectionPool;
    private final DelegatedContext<Member>                                    context;
    private final CommonCommunications<DhtService, ProtoKERLService>          dhtComms;
    private final double                                                      fpr;
    private final Duration                                                    operationsFrequency;
    private final CachingKERL                                                 kerl;
    private final UniKERLDirectPooled                                         kerlPool;
    private final KerlSpace                                                   kerlSpace;
    private final SigningMember                                               member;
    private final CommonCommunications<ReconciliationService, Reconciliation> reconcileComms;
    private final Reconcile                                                   reconciliation = new Reconcile();
    private final ScheduledExecutorService                                    scheduler;
    private final Service                                                     service        = new Service();
    private final AtomicBoolean                                               started        = new AtomicBoolean();
    private final Duration                                                    operationTimeout;

    public KerlDHT(Duration operationsFrequency, Context<? extends Member> context, SigningMember member,
                   BiFunction<KerlDHT, KERL.AppendKERL, KERL.AppendKERL> wrap, JdbcConnectionPool connectionPool,
                   DigestAlgorithm digestAlgorithm, Router communications, Duration operationTimeout,
                   double falsePositiveRate, StereotomyMetrics metrics) {
        assert member != null;
        this.context = new DelegatedContext<>((Context<Member>) new StaticContext<>(context));
        this.member = member;
        this.operationTimeout = operationTimeout;
        this.fpr = falsePositiveRate;
        this.operationsFrequency = operationsFrequency;
        this.scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        var kerlAdapter = new KERLAdapter(this, digestAlgorithm);
        this.cache = new CachingKERL(f -> {
            try {
                return f.apply(kerlAdapter);
            } catch (Throwable t) {
                log.error("error applying cache on: {}", member.getId(), t);
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
        this.kerlSpace = new KerlSpace(connectionPool, member.getId(), digestAlgorithm);

        initializeSchema();
        kerl = new CachingKERL(f -> {
            try (var k = kerlPool.create()) {
                return f.apply(wrap.apply(this, wrap(k)));
            } catch (Throwable e) {
                log.error("Cannot apply kerl on: {}", member.getId(), e);
                return null;
            }
        });
        this.ani = new Ani(member.getId(), asKERL());
    }

    public KerlDHT(Duration operationsFrequency, Context<? extends Member> context, SigningMember member,
                   JdbcConnectionPool connectionPool, DigestAlgorithm digestAlgorithm, Router communications,
                   Duration operationTimeout, double falsePositiveRate, StereotomyMetrics metrics) {
        this(operationsFrequency, context, member, (t, k) -> k, connectionPool, digestAlgorithm, communications,
             operationTimeout, falsePositiveRate, metrics);
    }

    public static void updateLocationHash(Identifier identifier, DigestAlgorithm digestAlgorithm, DSLContext dsl) {
        dsl.transaction(config -> {
            var context = DSL.using(config);
            var identBytes = b64(identifier.toIdent());
            // Braindead, but correct
            var id = context.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)).fetchOne();
            if (id == null) {
                throw new IllegalStateException("Identifier: %s not found".formatted(identifier));
            }

            var hashed = digestAlgorithm.digest(identBytes);
            context.insertInto(IDENTIFIER_LOCATION_HASH, IDENTIFIER_LOCATION_HASH.IDENTIFIER,
                               IDENTIFIER_LOCATION_HASH.DIGEST)
                   .values(id.value1(), b64(hashed.getBytes()))
                   .onDuplicateKeyIgnore()
                   .execute();
        });
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
        var slice = context.bftSubset(identifier);
        var iterator = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        try {
            iterator.iterate((link) -> link.append(Collections.emptyList(), Collections.singletonList(event)),
                             (futureSailor, tally, link, _) -> mutate(gathered, futureSailor, identifier, isTimedOut,
                                                                      tally, link, "append events"),
                             () -> completeIt(result, gathered), operationsFrequency);
            List<KeyState_> s = result.get().getKeyStatesList();
            return s.isEmpty() ? null : s.getFirst();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.info("error appending event: {} on: {}", ce.getMessage(), member.getId());
                return KeyState_.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public List<KeyState_> append(KERL_ kerl) {
        if (kerl.getEventsList().isEmpty()) {
            return Collections.emptyList();
        }
        final var event = kerl.getEventsList().getFirst();
        Digest identifier = digestOf(event, digestAlgorithm());
        if (identifier == null) {
            return Collections.emptyList();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStates>();
        HashMultiset<KeyStates> gathered = HashMultiset.create();
        var slice = context.bftSubset(identifier);
        var iterator = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        try {
            iterator.iterate((link) -> link.append(kerl),
                             (futureSailor, tally, link, _) -> mutate(gathered, futureSailor, identifier, isTimedOut,
                                                                      tally, link, "append kerl"),
                             () -> completeIt(result, gathered), operationsFrequency);
            return result.get().getKeyStatesList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error appending KERL: {} on: {}", ce.getMessage(), member.getId());
                return Collections.emptyList();
            }
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
        var slice = context.bftSubset(identifier);
        var iterator = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        try {
            iterator.iterate((link) -> link.append(Collections.singletonList(event)),
                             (futureSailor, tally, link, _) -> mutate(gathered, futureSailor, identifier, isTimedOut,
                                                                      tally, link, "append kerl"),
                             () -> completeIt(result, gathered), operationsFrequency);
            var ks = result.get();
            return ks.getKeyStatesCount() == 0 ? KeyState_.getDefaultInstance() : ks.getKeyStatesList().getFirst();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error appending Key Event: {} on: {}", ce.getMessage(), member.getId());
                return KeyState_.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public List<KeyState_> append(List<KeyEvent_> events) {
        if (events.isEmpty()) {
            return Collections.emptyList();
        }
        List<KeyState_> states = new ArrayList<>();
        events.stream().map(this::append).forEach(states::add);
        return states;
    }

    @Override
    public List<KeyState_> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
        if (events.isEmpty()) {
            return Collections.emptyList();
        }
        List<KeyState_> states = new ArrayList<>();
        events.stream().map(this::append).forEach(states::add);

        attachments.forEach(this::append);
        return states;
    }

    @Override
    public Empty appendAttachments(List<AttachmentEvent> events) {
        if (events.isEmpty()) {
            return Empty.getDefaultInstance();
        }
        final var event = events.getFirst();
        Digest identifier = digestAlgorithm().digest(event.getCoordinates().getIdentifier().toByteString());
        if (identifier == null) {
            return Empty.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Empty>();
        HashMultiset<Empty> gathered = HashMultiset.create();
        var slice = context.bftSubset(identifier);
        var iterator = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        try {
            iterator.iterate((link) -> link.appendAttachments(events),
                             (futureSailor, tally, link, _) -> mutate(gathered, futureSailor, identifier, isTimedOut,
                                                                      tally, link, "append kerl"),
                             () -> completeIt(result, gathered), operationsFrequency);
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error appending attachments: {} on: {}", ce.getMessage(), member.getId());
                return Empty.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public Empty appendValidations(Validations validations) {
        if (validations.getValidationsCount() == 0) {
            return null;
        }
        Digest identifier = digestAlgorithm().digest(validations.getCoordinates().getIdentifier().toByteString());
        if (identifier == null) {
            return null;
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Empty>();
        HashMultiset<Empty> gathered = HashMultiset.create();
        var slice = context.bftSubset(identifier);
        var iterator = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        try {
            iterator.iterate((link) -> link.appendValidations(validations),
                             (futureSailor, tally, link, _) -> mutate(gathered, futureSailor, identifier, isTimedOut,
                                                                      tally, link, "append kerl"),
                             () -> completeIt(result, gathered), operationsFrequency);
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error appending validations: {} on: {}", ce.getMessage(), member.getId());
                return Empty.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    public KERL.AppendKERL asKERL() {
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
            return Attachment.getDefaultInstance();
        }
        Digest identifier = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (identifier == null) {
            return Attachment.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Attachment>();
        HashMultiset<Attachment> gathered = HashMultiset.create();
        var operation = "getAttachment(%s)".formatted(EventCoordinates.from(coordinates));
        var slice = context.bftSubset(identifier);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getAttachment(coordinates),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, identifier,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error {} : {} on: {}", operation, ce.getMessage(), member.getId());
                return null;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KERL_ getKERL(Ident identifier) {
        if (identifier == null) {
            return KERL_.getDefaultInstance();
        }
        Digest digest = digestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return KERL_.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KERL_>();
        HashMultiset<KERL_> gathered = HashMultiset.create();
        var operation = "getKerl(%s)".formatted(Identifier.from(identifier));
        var slice = context.bftSubset(digest);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getKERL(identifier),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error {} : {} on: {}", operation, ce.getMessage(), member.getId());
                return KERL_.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyEvent_ getKeyEvent(EventCoords coordinates) {
        if (!coordinates.isInitialized()) {
            return KeyEvent_.getDefaultInstance();
        }
        var operation = "getKeyEvent(%s)".formatted(EventCoordinates.from(coordinates));
        log.trace("{} on: {}", operation, member.getId());
        if (coordinates == null) {
            return KeyEvent_.getDefaultInstance();
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return KeyEvent_.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyEvent_>();
        HashMultiset<KeyEvent_> gathered = HashMultiset.create();
        var slice = context.bftSubset(digest);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getKeyEvent(coordinates),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.info("error {} : {} on: {}", operation, ce.getMessage(), member.getId());
                return KeyEvent_.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyState_ getKeyState(EventCoords coordinates) {
        var operation = "getKeyState(%s)".formatted(EventCoordinates.from(coordinates));
        log.info("{} on: {}", operation, member.getId());
        if (coordinates == null) {
            return KeyState_.getDefaultInstance();
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return KeyState_.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        HashMultiset<KeyState_> gathered = HashMultiset.create();
        var slice = context.bftSubset(digest);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getKeyState(coordinates),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error {} : {} on: {}", operation, ce.getMessage(), member.getId());
                return KeyState_.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyState_ getKeyState(Ident identifier, long sequenceNumber) {
        var operation = "getKeyState(%s, %s)".formatted(Identifier.from(identifier), ULong.valueOf(sequenceNumber));
        log.warn("{} on: {}", operation, member.getId());
        if (identifier == null) {
            return KeyState_.getDefaultInstance();
        }
        Digest digest = digestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return KeyState_.getDefaultInstance();
        }
        var identAndSeq = IdentAndSeq.newBuilder().setIdentifier(identifier).setSequenceNumber(sequenceNumber).build();
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        HashMultiset<KeyState_> gathered = HashMultiset.create();
        var slice = context.bftSubset(digest);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getKeyState(identAndSeq),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error {} : {} on: {}", operation, ce.getMessage(), member.getId());
                return KeyState_.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyState_ getKeyState(Ident identifier) {
        if (identifier == null) {
            return KeyState_.getDefaultInstance();
        }
        var operation = "getKeyState(%s)".formatted(Identifier.from(identifier));
        log.info("{} on: {}", operation, member.getId());
        Digest digest = digestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return KeyState_.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        HashMultiset<KeyState_> gathered = HashMultiset.create();
        var slice = context.bftSubset(digest);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getKeyState(identifier),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error {} : {} on: {}", operation, ce.getMessage(), member.getId());
                return KeyState_.getDefaultInstance();
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coordinates) {
        var operation = "getKeyStateWithAttachments(%s)".formatted(EventCoordinates.from(coordinates));
        log.info("{} on: {}", operation, member.getId());
        if (coordinates == null) {
            return KeyStateWithAttachments_.getDefaultInstance();
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return KeyStateWithAttachments_.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStateWithAttachments_>();
        HashMultiset<KeyStateWithAttachments_> gathered = HashMultiset.create();
        var slice = context.bftSubset(digest);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getKeyStateWithAttachments(coordinates),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error {} on: {}", operation, member.getId(), ce);
                return null;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
        var operation = "getKeyStateWithEndorsementsAndValidations(%s)".formatted(EventCoordinates.from(coordinates));
        log.info("{} on: {}", operation, member.getId());
        if (coordinates == null) {
            return KeyStateWithEndorsementsAndValidations_.getDefaultInstance();
        }
        Digest digest = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return KeyStateWithEndorsementsAndValidations_.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyStateWithEndorsementsAndValidations_>();
        HashMultiset<KeyStateWithEndorsementsAndValidations_> gathered = HashMultiset.create();
        var slice = context.bftSubset(digest);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getKeyStateWithEndorsementsAndValidations(coordinates),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, digest,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error {} : {} on: {}", operation, ce.getMessage(), member.getId());
                return null;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public Validations getValidations(EventCoords coordinates) {
        var operation = "getValidations(%s)".formatted(EventCoordinates.from(coordinates));
        log.info("{} on: {}", operation, member.getId());
        if (coordinates == null) {
            return Validations.getDefaultInstance();
        }
        Digest identifier = digestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (identifier == null) {
            return Validations.getDefaultInstance();
        }
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Validations>();
        HashMultiset<Validations> gathered = HashMultiset.create();
        var slice = context.bftSubset(identifier);
        var iter = new SliceIterator<>(context.getId().toString(), member, slice, dhtComms, scheduler);
        iter.iterate(link -> link.getValidations(coordinates),
                     (futureSailor, tally, destination, _) -> read(result, gathered, tally, futureSailor, identifier,
                                                                   isTimedOut, destination, operation),
                     () -> failedMajority(result, maxCount(gathered), operation), operationsFrequency);
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CompletionException ce) {
                log.warn("error {} : {} on: {}", operation, ce.getMessage(), member.getId());
                return null;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    public Verifiers getVerifiers() {
        return new Verifiers() {
            @Override
            public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
                return verifierFor(coordinates.getIdentifier());
            }

            @Override
            public Optional<Verifier> verifierFor(Identifier identifier) {
                return Optional.of(new KerlVerifier<>(identifier, asKERL()));
            }
        };
    }

    public void nextView(ViewChange viewChange) {
        log.info("Next view: {} context: {} on: {}", viewChange.diadem(), viewChange.context().getId(), member.getId());
        context.setContext(viewChange.context());
    }

    public void start(Duration duration) {
        start(duration, null);
    }

    public void start(Duration duration, Predicate<FernetServerInterceptor.HashedToken> validator) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        dhtComms.register(context.getId(), service, validator);
        reconcileComms.register(context.getId(), reconciliation, validator);
        schedule(duration);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        scheduler.shutdownNow();
        dhtComms.deregister(context.getId());
        reconcileComms.deregister(context.getId());
    }

    private <T> T complete(Function<ProtoKERLAdapter, T> func) {
        try {
            return func.apply(new ProtoKERLAdapter(kerl));
        } catch (Throwable t) {
            log.error("Error completing on: {}", member.getId(), t);
            return null;
        }
    }

    private <T> void completeIt(CompletableFuture<T> result, HashMultiset<T> gathered) {
        var max = gathered.entrySet()
                          .stream()
                          .max(Ordering.natural().onResultOf(Multiset.Entry::getCount))
                          .orElse(null);
        var majority = context.size() == 1 ? 1 : context.toleranceLevel() + 1;
        if (max != null) {
            if (max.getCount() >= majority) {
                try {
                    result.complete(max.getElement());
                } catch (Throwable t) {
                    log.error("Unable to complete it on {}", member.getId(), t);
                }
                return;
            }
        } else {
            log.warn("Unable to achieve majority, max agree: 0 required: {}", majority + " on: {}", member.getId());
        }
        result.completeExceptionally(new CompletionException(
        "Unable to achieve majority, max: " + (max == null ? 0 : max.getCount()) + " required: " + majority + " on: "
        + member.getId()));
    }

    private boolean failedMajority(CompletableFuture<?> result, int maxAgree, String operation) {
        log.debug("Unable to achieve majority read: {}, max agree: {} required: {} on: {}", operation, maxAgree,
                  context.toleranceLevel() + 1, member.getId());
        return result.completeExceptionally(new CompletionException(
        "Unable to achieve majority read: " + operation + ", max agree: " + maxAgree + " required: "
        + context.toleranceLevel() + 1 + " on: " + member.getId()));
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
                log.error("Unable to initialize schema on: {}", member.getId(), e);
                throw new IllegalStateException(e);
            }
        } catch (SQLException e) {
            log.error("Unable to initialize schema on: {}", member.getId(), e);
            throw new IllegalStateException(e);
        }
    }

    private CombinedIntervals keyIntervals() {
        List<KeyInterval> intervals = new ArrayList<>();
        for (int i = 0; i < context.getRingCount(); i++) {
            Member predecessor = context.predecessor(i, member);
            if (predecessor == null) {
                continue;
            }

            Digest begin = context.hashFor(predecessor, i);
            Digest end = context.hashFor(member, i);

            if (begin.compareTo(end) > 0) { // wrap around the origin of the ring
                intervals.add(new KeyInterval(end, digestAlgorithm().getLast()));
                intervals.add(new KeyInterval(digestAlgorithm().getOrigin(), begin));
            } else {
                intervals.add(new KeyInterval(begin, end));
            }
        }
        return new CombinedIntervals(intervals);
    }

    private <T> Entry<T> max(HashMultiset<T> gathered) {
        return gathered.entrySet().stream().max(Ordering.natural().onResultOf(Multiset.Entry::getCount)).orElse(null);
    }

    private int maxCount(HashMultiset<?> gathered) {
        final var max = gathered.entrySet().stream().max(Ordering.natural().onResultOf(Multiset.Entry::getCount));
        return max.map(Entry::getCount).orElse(0);
    }

    private <T> boolean mutate(HashMultiset<T> gathered, Optional<T> futureSailor, Digest identifier,
                               Supplier<Boolean> isTimedOut, AtomicInteger tally, DhtService destination,
                               String action) {
        if (futureSailor.isEmpty()) {
            log.debug("Failed {}: {} tally: {} from: {}  on: {}", action, identifier, tally.get(),
                      destination.getMember() == null ? "<null>" : destination.getMember().getId(), member.getId());
            return !isTimedOut.get();
        }
        T content = futureSailor.get();
        gathered.add(content);
        gathered.entrySet()
                .stream()
                .max(Ordering.natural().onResultOf(Entry::getCount))
                .ifPresent(max -> tally.set(max.getCount()));
        log.warn("{}: {} tally: {} from: {} on: {}", action, identifier, tally.get(), destination.getMember().getId(),
                 member.getId());
        return !isTimedOut.get();
    }

    private <T> boolean read(CompletableFuture<T> result, HashMultiset<T> gathered, AtomicInteger tally,
                             Optional<T> futureSailor, Digest identifier, Supplier<Boolean> isTimedOut,
                             DhtService destination, String action) {
        if (futureSailor.isEmpty()) {
            log.debug("Failed {}: {} tally: {} from: {}  on: {}", action, identifier, tally,
                      destination.getMember() == null ? "<null>" : destination.getMember().getId(), member.getId());
            return !isTimedOut.get();
        }
        T content = futureSailor.get();
        log.trace("{}: {} tally: {} from: {}  on: {}", action, identifier, tally.get(), destination.getMember().getId(),
                  member.getId());
        gathered.add(content);
        var max = max(gathered);
        if (max != null) {
            tally.set(max.getCount());
            var ctxMajority = context.size() == 1 ? 1 : context.toleranceLevel() + 1;
            final var majority = tally.get() >= ctxMajority;
            if (majority) {
                result.complete(max.getElement());
                log.debug("Majority: {} achieved: {}: {} tally: {} on: {}", max.getCount(), action, identifier,
                          tally.get(), member.getId());
                return false;
            } else {
                log.info("Majority: {} required: {} not achieved: {}: {} tally: {} on: {}", max.getCount(), ctxMajority,
                         action, identifier, tally.get(), member.getId());
            }
        }
        return !isTimedOut.get();
    }

    private void reconcile(Update update, ReconciliationService link) {
        if (!started.get()) {
            return;
        }
        try {
            if (update.getEventsCount() > 0) {
                reconcileLog.trace("Received: {} events in interval reconciliation from: {} on: {}",
                                   update.getEventsCount(), link.getMember().getId(), member.getId());
                kerlSpace.update(update.getEventsList(), kerl);
            }
        } catch (NoSuchElementException e) {
            reconcileLog.debug("null interval reconciliation with {} : {} on: {}", link.getMember().getId(),
                               e.getMessage(), member.getId());
        }
    }

    private Update reconcile(ReconciliationService link, Integer ring) {
        if (member.equals(link.getMember())) {
            return null;
        }
        CombinedIntervals keyIntervals = keyIntervals();
        reconcileLog.trace("Interval reconciliation on ring: {} with: {} intervals: {} on: {} ", ring,
                           link.getMember().getId(), keyIntervals, member.getId());
        return link.reconcile(Intervals.newBuilder()
                                       .setRing(ring)
                                       .addAllIntervals(keyIntervals.toIntervals())
                                       .setHave(kerlSpace.populate(Entropy.nextBitsStreamLong(), keyIntervals, fpr))
                                       .build());
    }

    private void reconcile(Duration duration) {
        if (!started.get()) {
            return;
        }
        var successors = context.successors(member.getId(), m -> true, member);
        Collections.shuffle(successors);
        successors.forEach(i -> {
            try (var link = reconcileComms.connect(i.m())) {
                if (link != null) {
                    reconcile(reconcile(link, i.ring()), link);
                }
                try {
                    Thread.sleep(duration.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } catch (IOException e) {
                log.debug("Error reconciling with: {} on: {}", i.m(), member.getId(), e);
            } finally {
                schedule(duration);
            }
        });
    }

    private void schedule(Duration duration) {
        Thread.ofVirtual().start(() -> Utils.wrapped(() -> reconcile(duration), log));
    }

    private void updateLocationHash(Identifier identifier) {
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection, SQLDialect.H2);
            updateLocationHash(identifier, kerl.getDigestAlgorithm(), dsl);
        } catch (SQLException e) {
            log.error("Cannot update location hash for: {} on: {}", identifier, member.getId());
            throw new IllegalStateException(
            "Cannot update location hash S for: %s on: %s".formatted(identifier, member.getId()));
        } catch (DataAccessException e) {
            log.trace("Duplicate location hash for: {} on: {}", identifier, member.getId());
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
        Member successor = context.successor(ring, fromMember);
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
                if (!lks.isEmpty()) {
                    updateLocationHash(lks.getFirst().getCoordinates().getIdentifier());
                }
                return lks;
            }

            @Override
            public List<KeyState> append(List<KeyEvent> events,
                                         List<com.salesforce.apollo.stereotomy.event.AttachmentEvent> attachments) {
                List<KeyState> lks = super.append(events, attachments);
                if (!lks.isEmpty()) {
                    updateLocationHash(lks.getFirst().getCoordinates().getIdentifier());
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
            log.debug("appending kerl on: {}", member.getId());
            return complete(k -> k.append(kerl_));
        }

        @Override
        public List<KeyState_> append(List<KeyEvent_> events) {
            log.debug("appending events on: {}", member.getId());
            return complete(k -> k.append(events));
        }

        @Override
        public List<KeyState_> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
            log.debug("appending events and attachments on: {}", member.getId());
            return complete(k -> k.append(events, attachments));
        }

        @Override
        public Empty appendAttachments(List<AttachmentEvent> attachments) {
            log.debug("append attachments on: {}", member.getId());
            return complete(k -> k.appendAttachments(attachments));
        }

        @Override
        public Empty appendValidations(Validations validations) {
            log.debug("append validations on: {}", member.getId());
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
            final Function<ProtoKERLAdapter, KeyEvent_> func = k -> k.getKeyEvent(coordinates);
            return complete(func);
        }

        @Override
        public KeyState_ getKeyState(EventCoords coordinates) {
            log.trace("get key state for coordinates on: {}", member.getId());
            return complete(k -> k.getKeyState(coordinates));
        }

        @Override
        public KeyState_ getKeyState(Ident identifier, long sequenceNumber) {
            if (log.isTraceEnabled()) {
                log.trace("get key state for {}:{} on: {}", Identifier.from(identifier), ULong.valueOf(sequenceNumber),
                          member.getId());
            }
            return complete(k -> k.getKeyState(identifier, sequenceNumber));
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
