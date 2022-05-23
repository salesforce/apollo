/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.h2.jdbc.JdbcConnection;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.event.proto.RotationEvent;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesfoce.apollo.thoth.proto.Update;
import com.salesfoce.apollo.thoth.proto.Updating;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingIterator;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.db.UniKERLDirect;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import com.salesforce.apollo.thoth.grpc.Reconciliation;
import com.salesforce.apollo.thoth.grpc.ReconciliationClient;
import com.salesforce.apollo.thoth.grpc.ReconciliationServer;
import com.salesforce.apollo.thoth.grpc.ReconciliationService;
import com.salesforce.apollo.thoth.grpc.ThothClient;
import com.salesforce.apollo.thoth.grpc.ThothServer;
import com.salesforce.apollo.thoth.grpc.ThothService;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * Thoth provides the replicated state store for KERLs
 *
 * @author hal.hildebrand
 *
 */
public class Thoth {
    public static class MajorityWriteFail extends Exception {

        private static final long serialVersionUID = 1L;

        public MajorityWriteFail(String message) {
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

            return Thoth.this.kerlSpace.reconcile(intervals);
        }

        @Override
        public void update(Updating update, Digest from) {
            var ring = update.getRing();
            if (!valid(from, ring)) {
                return;
            }
            Thoth.this.kerlSpace.update(update.getEventsList());
        }
    }

    private class Service implements ProtoKERLService {

        @Override
        public CompletableFuture<List<KeyState_>> append(KERL_ kerl) {
            return Thoth.this.kerl.append(kerl);
        }

        @Override
        public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events) {
            return Thoth.this.kerl.append(events);
        }

        @Override
        public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events,
                                                         List<com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent> attachments) {
            return Thoth.this.kerl.append(events, attachments);
        }

        @Override
        public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
            return Thoth.this.kerl.getAttachment(coordinates);
        }

        @Override
        public CompletableFuture<KERL_> getKERL(Ident identifier) {
            return Thoth.this.kerl.getKERL(identifier);
        }

        @Override
        public CompletableFuture<KeyEvent_> getKeyEvent(Digeste digest) {
            return Thoth.this.kerl.getKeyEvent(digest);
        }

        @Override
        public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
            return Thoth.this.kerl.getKeyEvent(coordinates);
        }

        @Override
        public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
            return Thoth.this.kerl.getKeyState(coordinates);
        }

        @Override
        public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
            return Thoth.this.kerl.getKeyState(identifier);
        }
    }

    private final static Logger log = LoggerFactory.getLogger(Thoth.class);

    private static <T> CompletableFuture<T> complete(T value) {
        var fs = new CompletableFuture<T>();
        fs.complete(value);
        return fs;
    }

    private final JdbcConnection                                              connection;
    private final Context<Member>                                             context;
    private final Executor                                                    executor;
    private final ProtoKERLAdapter                                            kerl;
    private final SigningMember                                               member;
    private final RingCommunications<ReconciliationService>                   reconcile;
    private final CommonCommunications<ReconciliationService, Reconciliation> reconcileComms;
    private final Reconcile                                                   reconciliation = new Reconcile();
    private final Service                                                     service        = new Service();
    private final AtomicBoolean                                               started        = new AtomicBoolean();
    private final CommonCommunications<ThothService, ProtoKERLService>        thothComms;
    private final TemporalAmount                                              timeout;
    private final double                                                      fpr;
    private final KERLSpace                                                   kerlSpace;

    public Thoth(Context<Member> context, SigningMember member, JdbcConnection connection,
                 DigestAlgorithm digestAlgorithm, Router communications, Executor executor, TemporalAmount timeout,
                 double falsePositiveRate, StereotomyMetrics metrics) {
        this.context = context;
        this.member = member;
        this.timeout = timeout;
        this.fpr = falsePositiveRate;
        thothComms = communications.create(member, context.getId(), service, r -> new ThothServer(r, executor, metrics),
                                           ThothClient.getCreate(context.getId(), metrics),
                                           ThothClient.getLocalLoopback(service, member));
        reconcileComms = communications.create(member, context.getId(), reconciliation,
                                               r -> new ReconciliationServer(r,
                                                                             communications.getClientIdentityProvider(),
                                                                             executor, metrics),
                                               ReconciliationClient.getCreate(context.getId(), metrics),
                                               ReconciliationClient.getLocalLoopback(reconciliation, member));
        this.connection = connection;
        this.kerl = new ProtoKERLAdapter(new UniKERLDirect(connection, digestAlgorithm));
        this.executor = executor;
        this.reconcile = new RingCommunications<>(context, member, reconcileComms, executor);
        this.kerlSpace = new KERLSpace(DSL.using(connection));

        initializeSchema();
    }

    public CompletableFuture<Void> append(KERL_ kerl) {
        if (kerl.getEventsList().isEmpty()) {
            return complete(null);
        }
        final var event = kerl.getEventsList().get(0);
        Digest identifier = digestOf(event);
        if (identifier == null) {
            return complete(null);
        }
        CompletableFuture<Boolean> majority = new CompletableFuture<>();
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        new RingIterator<>(context, member, thothComms,
                           executor).iterate(identifier, () -> majority.complete(true), (link, r) -> link.append(kerl),
                                             () -> majority.complete(false),
                                             (tally, futureSailor, link, r) -> mutate(futureSailor, identifier,
                                                                                      isTimedOut, tally, link,
                                                                                      "append kerl"),
                                             null);
        return complete(majority, null);
    }

    public CompletableFuture<Void> append(List<KeyEvent_> events) {
        if (events.isEmpty()) {
            return complete(null);
        }
        final var event = events.get(0);
        Digest identifier = digestOf(event);
        if (identifier == null) {
            return complete(null);
        }
        CompletableFuture<Boolean> majority = new CompletableFuture<>();
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        new RingIterator<>(context, member, thothComms,
                           executor).iterate(identifier, () -> majority.complete(true),
                                             (link, r) -> link.append(events), () -> majority.complete(false),
                                             (tally, futureSailor, link, r) -> mutate(futureSailor, identifier,
                                                                                      isTimedOut, tally, link,
                                                                                      "append events"),
                                             null);
        return complete(majority, null);
    }

    public CompletableFuture<Void> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
        if (events.isEmpty()) {
            return complete(null);
        }
        final var event = events.get(0);
        Digest identifier = digestOf(event);
        if (identifier == null) {
            return complete(null);
        }
        CompletableFuture<Boolean> majority = new CompletableFuture<>();
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        new RingIterator<>(context, member, thothComms,
                           executor).iterate(identifier, () -> majority.complete(true),
                                             (link, r) -> link.append(events, attachments),
                                             () -> majority.complete(false),
                                             (tally, futureSailor, link, r) -> mutate(futureSailor, identifier,
                                                                                      isTimedOut, tally, link,
                                                                                      "append events"),
                                             null);
        return complete(majority, null);
    }

    public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
        if (coordinates == null) {
            return complete(null);
        }
        Digest identifier = kerl.getDigestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (identifier == null) {
            return complete(null);
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Attachment>();
        new RingIterator<>(context, member, thothComms, executor).iterate(identifier,
                                                                          (link, r) -> link.getAttachment(coordinates),
                                                                          (tally, futureSailor, link,
                                                                           r) -> read(result, futureSailor, identifier,
                                                                                      isTimedOut, link,
                                                                                      "get attachment"));
        return result;
    }

    public CompletableFuture<KERL_> getKERL(Ident identifier) {
        if (identifier == null) {
            return complete(null);
        }
        Digest digest = kerl.getDigestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return complete(null);
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KERL_>();
        new RingIterator<>(context, member, thothComms, executor).iterate(digest, (link, r) -> link.getKERL(identifier),
                                                                          (tally, futureSailor, link,
                                                                           r) -> read(result, futureSailor, digest,
                                                                                      isTimedOut, link, "get kerl"));
        return result;
    }

    public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        if (coordinates == null) {
            return complete(null);
        }
        Digest digest = kerl.getDigestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return complete(null);
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyEvent_>();
        new RingIterator<>(context, member, thothComms,
                           executor).iterate(digest, (link, r) -> link.getKeyEvent(coordinates),
                                             (tally, futureSailor, link, r) -> read(result, futureSailor, digest,
                                                                                    isTimedOut, link, "get key event"));
        return result;
    }

    public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
        if (coordinates == null) {
            return complete(null);
        }
        Digest digest = kerl.getDigestAlgorithm().digest(coordinates.getIdentifier().toByteString());
        if (digest == null) {
            return complete(null);
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        new RingIterator<>(context, member, thothComms, executor).iterate(digest,
                                                                          (link, r) -> link.getKeyState(coordinates),
                                                                          (tally, futureSailor, link,
                                                                           r) -> read(result, futureSailor, digest,
                                                                                      isTimedOut, link,
                                                                                      "get attachment"));
        return result;
    }

    public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
        if (identifier == null) {
            return complete(null);
        }
        Digest digest = kerl.getDigestAlgorithm().digest(identifier.toByteString());
        if (digest == null) {
            return complete(null);
        }
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<KeyState_>();
        new RingIterator<>(context, member, thothComms, executor).iterate(digest,
                                                                          (link, r) -> link.getKeyState(identifier),
                                                                          (tally, futureSailor, link,
                                                                           r) -> read(result, futureSailor, digest,
                                                                                      isTimedOut, link,
                                                                                      "get attachment"));
        return result;
    }

    public void start(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        thothComms.register(context.getId(), service);
        reconcileComms.register(context.getId(), reconciliation);
        reconcile(scheduler, duration);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        thothComms.deregister(context.getId());
        reconcileComms.deregister(context.getId());
    }

    private <T> CompletableFuture<T> complete(CompletableFuture<Boolean> majority, AtomicReference<T> result) {
        return majority.thenCompose(b -> {
            var fs = new CompletableFuture<T>();
            if (!b) {
                fs.completeExceptionally(new MajorityWriteFail("Unable to complete majority write"));
            } else {
                fs.complete(result.get());
            }
            return fs;
        });
    }

    private Digest digestOf(InceptionEvent event) {
        return this.kerl.getDigestAlgorithm().digest(event.getIdentifier().toByteString());
    }

    private Digest digestOf(InteractionEvent event) {
        return this.kerl.getDigestAlgorithm()
                        .digest(event.getSpecification().getHeader().getIdentifier().toByteString());
    }

    private Digest digestOf(final KeyEvent_ event) {
        return switch (event.getEventCase()) {
        case INCEPTION -> digestOf(event.getInception());
        case INTERACTION -> digestOf(event.getInteraction());
        case ROTATION -> digestOf(event.getRotation());
        default -> null;
        };
    }

    private Digest digestOf(final KeyEventWithAttachments event) {
        return switch (event.getEventCase()) {
        case INCEPTION -> digestOf(event.getInception());
        case INTERACTION -> digestOf(event.getInteraction());
        case ROTATION -> digestOf(event.getRotation());
        default -> null;
        };
    }

    private Digest digestOf(RotationEvent event) {
        return this.kerl.getDigestAlgorithm()
                        .digest(event.getSpecification().getHeader().getIdentifier().toByteString());
    }

    private void initializeSchema() {
        var database = new H2Database() {
            @Override
            public void close() throws DatabaseException {
                // Don't close the connection
            }
        };
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/initialize-thoth.xml", new ClassLoaderResourceAccessor(),
                                                 database)) {
            liquibase.update((String) null);
        } catch (LiquibaseException e) {
            throw new IllegalStateException(e);
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
                intervals.add(new KeyInterval(end, kerl.getDigestAlgorithm().getLast()));
                intervals.add(new KeyInterval(kerl.getDigestAlgorithm().getOrigin(), begin));
            } else {
                intervals.add(new KeyInterval(begin, end));
            }
        }
        return new CombinedIntervals(intervals);
    }

    private boolean mutate(Optional<ListenableFuture<Empty>> futureSailor, Digest identifier,
                           Supplier<Boolean> isTimedOut, AtomicInteger tally, ThothService link, String action) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        try {
            futureSailor.get().get();
        } catch (InterruptedException e) {
            log.warn("Error {}: {} from: {} on: {}", action, identifier, link.getMember(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) e.getCause();
                if (sre.getStatus() == Status.UNAVAILABLE) {
                    log.trace("Server unavailable action: {} from: {} on: {}", action, identifier, link.getMember(),
                              member);
                }
            } else {
                log.warn("Error {}: {} from: {} on: {}", action, identifier, link.getMember(), member, e.getCause());
            }
            return !isTimedOut.get();
        }
        log.trace("{}: {} on: {}", action, identifier, member);
        tally.incrementAndGet();
        return !isTimedOut.get();
    }

    private Biff populate(CombinedIntervals keyIntervals) {
        List<Digest> digests = kerlSpace.populate(keyIntervals);
        var biff = new DigestBloomFilter(Entropy.nextBitsStreamLong(), digests.size(), fpr);
        return biff.toBff();
    }

    private <T> boolean read(CompletableFuture<T> result, Optional<ListenableFuture<T>> futureSailor, Digest identifier,
                             Supplier<Boolean> isTimedOut, ThothService link, String action) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        T content;
        try {
            content = futureSailor.get().get();
        } catch (InterruptedException e) {
            log.debug("Error {}: {} from: {} on: {}", action, identifier, link.getMember(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) t;
                if (sre.getStatus() == Status.NOT_FOUND) {
                    log.trace("Error {}: {} server not found: {} on: {}", action, identifier, link.getMember(), member);
                    return !isTimedOut.get();
                }
            }
            log.debug("Error {}: {} from: {} on: {}", action, identifier, link.getMember(), member, e.getCause());
            return !isTimedOut.get();
        }
        if (content != null || (content != null && content.equals(Attachment.getDefaultInstance()))) {
            log.trace("{}: {} from: {}  on: {}", action, identifier, link.getMember(), member);
            result.complete(content);
            return false;
        } else {
            log.debug("Failed {}: {} from: {}  on: {}", action, identifier, link.getMember(), member);
            return !isTimedOut.get();
        }
    }

    private void reconcile(Optional<ListenableFuture<Update>> futureSailor, ReconciliationService link,
                           ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get() || futureSailor.isEmpty()) {
            return;
        }
        try {
            Update update = futureSailor.get().get();
            log.trace("Received: {} events in interval reconciliation from: {} on: {}", update.getEventsCount(),
                      link.getMember(), member);
            kerlSpace.update(update.getEventsList());
        } catch (InterruptedException | ExecutionException e) {
            log.debug("Error in interval reconciliation with {} : {}", link.getMember(), e.getCause());
        }
        if (started.get()) {
            scheduler.schedule(() -> reconcile(scheduler, duration), duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private ListenableFuture<Update> reconcile(ReconciliationService link, Integer ring) {
        CombinedIntervals keyIntervals = keyIntervals();
        log.info("Interval reconciliation on ring: {} with: {} on: {} intervals: {}", ring, link.getMember(), member,
                 keyIntervals);
        return link.reconcile(Intervals.newBuilder()
                                       .setContext(context.getId().toDigeste())
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
                          (futureSailor, link, ring) -> reconcile(futureSailor, link, scheduler, duration));

    }

    private boolean valid(Digest from, int ring) {
        if (ring >= context.getRingCount() || ring < 0) {
            log.warn("invalid ring {} from {}", ring, from);
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
