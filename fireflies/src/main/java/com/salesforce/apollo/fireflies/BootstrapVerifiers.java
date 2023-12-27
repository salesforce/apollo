package com.salesforce.apollo.fireflies;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.HashMultiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.fireflies.comm.entrance.EntranceClient;
import com.salesforce.apollo.fireflies.proto.IdentifierSequenceNumber;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.KeyStateVerifier;
import com.salesforce.apollo.stereotomy.Verifiers;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import io.grpc.StatusRuntimeException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Verifiers that delegate to the joining member's successors in the full context for key state retrieval
 * <p>
 * This is used to bootstrap the node via delegated key state resolution of the joined group
 * </p>
 *
 * @author hal.hildebrand
 **/
public class BootstrapVerifiers implements Verifiers {
    private final static Logger                                             log = LoggerFactory.getLogger(
    BootstrapVerifiers.class);
    private final        List<View.Participant>                             successors;
    private final        View.Node                                          member;
    private final        int                                                majority;
    private final        LoadingCache<EventCoordinates, KeyState>           ksCoords;
    private final        LoadingCache<IdentifierSequence, KeyState>         ksSeq;
    private final        RouterImpl.CommonCommunications<EntranceClient, ?> communications;
    private final        Duration                                           operationTimeout;
    private final        Duration                                           operationsFrequency;

    public BootstrapVerifiers(View.Node member, Duration operationTimeout, List<View.Participant> successors,
                              int majority, Duration operationsFrequency,
                              RouterImpl.CommonCommunications<EntranceClient, ?> communications) {
        this.member = member;
        this.successors = successors;
        this.majority = majority;
        this.communications = communications;
        this.operationTimeout = operationTimeout;
        this.operationsFrequency = operationsFrequency;
        ksCoords = Caffeine.newBuilder()
                           .maximumSize(10)
                           .expireAfterWrite(Duration.ofMinutes(10))
                           .removalListener((EventCoordinates coords, KeyState ks, RemovalCause cause) -> log.trace(
                           "KeyState {} was removed ({})", coords, cause))
                           .build(new CacheLoader<EventCoordinates, KeyState>() {

                               @Override
                               public @Nullable KeyState load(EventCoordinates key) throws Exception {
                                   return delegate(key);
                               }
                           });
        ksSeq = Caffeine.newBuilder()
                        .maximumSize(10)
                        .expireAfterWrite(Duration.ofMinutes(10))
                        .removalListener((IdentifierSequence seq, KeyState ks, RemovalCause cause) -> log.trace(
                        "KeyState {} was removed ({})", seq, cause))
                        .build(new CacheLoader<IdentifierSequence, KeyState>() {

                            @Override
                            public @Nullable KeyState load(IdentifierSequence key) throws Exception {
                                return delegate(key);
                            }
                        });
    }

    @Override
    public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
        return Optional.of(new BootstrapVerifier(coordinates.getIdentifier()));
    }

    @Override
    public Optional<Verifier> verifierFor(Identifier identifier) {
        return Optional.of(new BootstrapVerifier(identifier));
    }

    private <T> boolean complete(CompletableFuture<KeyState> ksFuture,
                                 Optional<ListenableFuture<KeyState_>> futureSailor, HashMultiset<KeyState_> keystates,
                                 Member m) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            final var ks = futureSailor.get().get();
            keystates.add(ks);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                case RESOURCE_EXHAUSTED:
                    log.trace("SRE in redirect: {} on: {}", sre.getStatus(), member.getId());
                    break;
                default:
                    log.trace("SRE in redirect: {} on: {}", sre.getStatus(), member.getId());
                }
            } else {
                log.error("Error in redirect: {} on: {}", ex.getCause(), member.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (CancellationException e) {
            // noop
        }

        var vs = keystates.entrySet()
                          .stream()
                          .filter(e -> e.getCount() >= majority)
                          .map(e -> e.getElement())
                          .findFirst()
                          .orElse(null);
        if (vs != null) {
            var keyState = new KeyStateImpl(vs);
            if (ksFuture.complete(keyState)) {
                log.debug("Key state: {} received majority on: {}", keyState.getCoordinates(), member.getId());
                return false;
            }
        }
        return true;
    }

    private KeyState delegate(IdentifierSequence idSeq) {
        var iterator = new SliceIterator<>("Retrieve KeyState", member, successors, communications);
        final var identifierSeq = idSeq.toIdSeq();
        var ks = new CompletableFuture<KeyState>();
        HashMultiset<KeyState_> keystates = HashMultiset.create();
        iterator.iterate((link, m) -> {
            log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), member.getId());
            return link.getKeyState(identifierSeq);
        }, (futureSailor, link, m) -> complete(ks, futureSailor, keystates, m), () -> {
            if (!ks.isDone()) {

            }
        }, Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()), Duration.ofMillis(10));
        try {
            return ks.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("Unable to retrieve key state: {} on: {}", idSeq, member.getId());
        }
        return null;
    }

    private KeyState delegate(EventCoordinates coordinates) {
        var iterator = new SliceIterator<>("Retrieve KeyState", member, successors, communications);
        final var coords = coordinates.toEventCoords();
        var ks = new CompletableFuture<KeyState>();
        HashMultiset<KeyState_> keystates = HashMultiset.create();
        iterator.iterate((link, m) -> {
            log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), member.getId());
            return link.getKeyState(coords);
        }, (futureSailor, link, m) -> complete(ks, futureSailor, keystates, m), () -> {
            if (!ks.isDone()) {

            }
        }, Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()), Duration.ofMillis(10));
        try {
            return ks.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("Unable to retrieve key state: {} on: {}", coordinates, member.getId());
        }
        return null;
    }

    private record IdentifierSequence(Identifier identifier, ULong seqNum) {
        public IdentifierSequenceNumber toIdSeq() {
            return IdentifierSequenceNumber.newBuilder()
                                           .setIdentifier(identifier.toIdent())
                                           .setSequenceNumber(seqNum.longValue())
                                           .build();
        }

        @Override
        public String toString() {
            return "{" + "identifier=" + identifier + ", seqNum=" + seqNum + '}';
        }
    }

    private class BootstrapVerifier extends KeyStateVerifier {

        public BootstrapVerifier(Identifier identifier) {
            super(identifier);
        }

        @Override
        protected KeyState getKeyState(ULong sequenceNumber) {
            return ksSeq.get(new IdentifierSequence(identifier, sequenceNumber));
        }

        @Override
        protected KeyState getKeyState(EventCoordinates coordinates) {
            return ksCoords.get(coordinates);
        }
    }
}
