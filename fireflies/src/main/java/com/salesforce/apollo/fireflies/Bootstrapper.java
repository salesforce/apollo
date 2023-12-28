package com.salesforce.apollo.fireflies;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.HashMultiset;
import com.google.protobuf.ByteString;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.fireflies.comm.entrance.Entrance;
import com.salesforce.apollo.fireflies.proto.Validation;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.stereotomy.*;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.proto.IdentAndSeq;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Verifiers that delegate to the joining member's successors in the full context for key state retrieval
 * <p>
 * This is used to bootstrap the node via delegated key state resolution of the joined group
 * </p>
 *
 * @author hal.hildebrand
 **/
public class Bootstrapper implements Verifiers {
    private final static Logger                                       log       = LoggerFactory.getLogger(
    Bootstrapper.class);
    private final        List<? extends Member>                       successors;
    private final        SigningMember                                member;
    private final        int                                          majority;
    private final        LoadingCache<EventCoordinates, KeyState>     ksCoords;
    private final        LoadingCache<IdentifierSequence, KeyState>   ksSeq;
    private final        RouterImpl.CommonCommunications<Entrance, ?> communications;
    private final        Duration                                     operationTimeout;
    private final        Duration                                     operationsFrequency;
    private final        ScheduledExecutorService                     scheduler = Executors.newScheduledThreadPool(1,
                                                                                                                   Thread.ofVirtual()
                                                                                                                         .factory());

    public <S extends SigningMember, M extends Member> Bootstrapper(S member, Duration operationTimeout,
                                                                    List<M> successors, int majority,
                                                                    Duration operationsFrequency,
                                                                    RouterImpl.CommonCommunications<Entrance, ?> communications) {
        this.member = member;
        this.successors = new ArrayList<>(successors);
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

    public EventValidation getValidator() {
        return new EventValidation() {
            @Override
            public Verifier.Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold,
                                              JohnHancock signature, InputStream message) {
                log.trace("Filtering for: {} on: {}", coordinates, member);
                var keyState = getKeyState(coordinates);
                if (keyState.isEmpty()) {
                    return new Verifier.Filtered(false, 0, null);
                }
                KeyState ks = keyState.get();
                var v = new Verifier.DefaultVerifier(ks.getKeys());
                return v.filtered(threshold, signature, message);
            }

            @Override
            public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
                log.trace("Get key state: {} on: {}", coordinates, member);
                return Optional.of(Bootstrapper.this.getKeyState(coordinates));
            }

            @Override
            public Optional<KeyState> getKeyState(Identifier identifier, ULong seqNum) {
                log.trace("Get key state: {}:{} on: {}", identifier, seqNum, member);
                return Optional.of(Bootstrapper.this.getKeyState(identifier, seqNum));
            }

            @Override
            public boolean validate(EstablishmentEvent event) {
                log.trace("Validate event: {} on: {}", event, member);
                return Bootstrapper.this.validate(event.getCoordinates());
            }

            @Override
            public boolean validate(EventCoordinates coordinates) {
                log.trace("Validating coordinates: {} on: {}", coordinates, member);
                return Bootstrapper.this.validate(coordinates);
            }

            @Override
            public boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message) {
                log.trace("Verify coordinates: {} on: {}", coordinates, member);
                return new BootstrapVerifier(coordinates.getIdentifier()).verify(signature, message);
            }

            @Override
            public boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                  InputStream message) {
                log.trace("Verify coordinates: {} on: {}", coordinates, member);
                return new BootstrapVerifier(coordinates.getIdentifier()).verify(threshold, signature, message);
            }
        };
    }

    @Override
    public Optional<Verifier> verifierFor(Identifier identifier) {
        return Optional.of(new BootstrapVerifier(identifier));
    }

    @Override
    public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
        return Optional.of(new BootstrapVerifier(coordinates.getIdentifier()));
    }

    protected KeyState getKeyState(Identifier identifier, ULong sequenceNumber) {
        return ksSeq.get(new IdentifierSequence(identifier, sequenceNumber));
    }

    private <T> boolean complete(CompletableFuture<KeyState> ksFuture, Optional<KeyState_> futureSailor,
                                 HashMultiset<KeyState_> keystates, Member m) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        if (ksFuture.isDone()) {
            return true;
        }
        final var ks = futureSailor.get();
        keystates.add(ks);

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

    private boolean completeValidation(CompletableFuture<Validation> valid, Optional<Validation> futureSailor,
                                       HashMultiset<Validation> validations, Member m) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        if (valid.isDone()) {
            return true;
        }
        final var v = futureSailor.get();
        validations.add(v);

        var validation = validations.entrySet()
                                    .stream()
                                    .filter(e -> e.getCount() >= majority)
                                    .map(e -> e.getElement())
                                    .findFirst();
        if (!validation.isEmpty()) {
            if (valid.complete(validation.get())) {
                log.debug("Validation: {} received majority on: {}", validation.get().getResult(), member.getId());
                return false;
            }
        }
        return true;
    }

    private KeyState delegate(EventCoordinates coordinates) {
        log.info("Get key state: {} from slice on: {}", coordinates, member);
        var iterator = new SliceIterator<>("Retrieve KeyState", member, successors, communications);
        final var coords = coordinates.toEventCoords();
        var ks = new CompletableFuture<KeyState>();
        HashMultiset<KeyState_> keystates = HashMultiset.create();
        iterator.iterate((link, m) -> {
            log.debug("Requesting Key State from: {} on: {}", link.getMember().getId(), member.getId());
            return link.getKeyState(coords);
        }, (futureSailor, link, m) -> complete(ks, futureSailor, keystates, m), () -> {
            if (!ks.isDone()) {
                log.warn("Failed to retrieve key state: {} from slice on: {}", coordinates, member);
                ks.complete(null);
            }
        }, scheduler, operationsFrequency);
        try {
            return ks.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("Unable to retrieve key state: {} on: {}", coordinates, member.getId());
        }
        return null;
    }

    private KeyState delegate(IdentifierSequence idSeq) {
        log.info("Get key state: {} from slice on: {}", idSeq, member);
        var iterator = new SliceIterator<>("Retrieve KeyState", member, successors, communications);
        final var identifierSeq = idSeq.toIdSeq();
        var ks = new CompletableFuture<KeyState>();
        HashMultiset<KeyState_> keystates = HashMultiset.create();
        iterator.iterate((link, m) -> {
            log.debug("Requesting Key State from: {} on: {}", link.getMember().getId(), member.getId());
            return link.getKeyState(identifierSeq);
        }, (futureSailor, link, m) -> complete(ks, futureSailor, keystates, m), () -> {
            if (!ks.isDone()) {
                log.warn("Failed to retrieve key state: {} from slice on: {}", idSeq, member);
                ks.complete(null);
            }
        }, scheduler, operationsFrequency);
        try {
            return ks.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("Unable to retrieve key state: {} on: {}", idSeq, member.getId());
        }
        return null;
    }

    private KeyState getKeyState(EventCoordinates coordinates) {
        return ksCoords.get(coordinates);
    }

    private boolean validate(EventCoordinates coordinates) {
        log.info("Validate event: {} from slice on: {}", coordinates, member);
        var succ = successors.stream().filter(m -> coordinates.getIdentifier().equals(m.getId())).findFirst();
        if (succ.isPresent()) {
            return true;
        }
        var iterator = new SliceIterator<>("Retrieve KeyState", member, successors, communications);
        var valid = new CompletableFuture<Validation>();
        HashMultiset<Validation> validations = HashMultiset.create();
        iterator.iterate((link, m) -> {
            log.debug("Requesting Validation: {} from: {} on: {}", coordinates, link.getMember().getId(),
                      member.getId());
            return link.validate(coordinates.toEventCoords());
        }, (futureSailor, link, m) -> completeValidation(valid, futureSailor, validations, m), () -> {
            if (!valid.isDone()) {
                log.warn("Failed to validate: {} from slice on: {}", coordinates, member);
                valid.complete(Validation.newBuilder().setResult(false).build());
            }
        }, scheduler, operationsFrequency);
        try {
            return valid.get().getResult();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("Unable to validate: {} on: {}", coordinates, member.getId());
        }
        return false;
    }

    public static class ViewEventValidation implements EventValidation {
        private EventValidation delegate;

        public ViewEventValidation(EventValidation delegate) {
            this.delegate = delegate;
        }

        @Override
        public Verifier.Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold,
                                          JohnHancock signature, InputStream message) {
            return delegate.filtered(coordinates, threshold, signature, message);
        }

        @Override
        public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
            return delegate.getKeyState(coordinates);
        }

        @Override
        public Optional<KeyState> getKeyState(Identifier identifier, ULong seqNum) {
            return delegate.getKeyState(identifier, seqNum);
        }

        @Override
        public boolean validate(EstablishmentEvent event) {
            return delegate.validate(event);
        }

        @Override
        public boolean validate(EventCoordinates coordinates) {
            return delegate.validate(coordinates);
        }

        @Override
        public boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                              InputStream message) {
            return delegate.verify(coordinates, threshold, signature, message);
        }

        @Override
        public boolean verify(EventCoordinates coordinates, JohnHancock signature, ByteString byteString) {
            return delegate.verify(coordinates, signature, byteString);
        }

        @Override
        public boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message) {
            return delegate.verify(coordinates, signature, message);
        }

        void setDelegate(EventValidation delegate) {
            this.delegate = delegate;
        }
    }

    private record IdentifierSequence(Identifier identifier, ULong seqNum) {
        public IdentAndSeq toIdSeq() {
            return IdentAndSeq.newBuilder()
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
            var key = new IdentifierSequence(identifier, sequenceNumber);
            log.info("Get key state: {} on: {}", key, member);
            return ksSeq.get(key);
        }

        @Override
        protected KeyState getKeyState(EventCoordinates coordinates) {
            log.info("Get key state: {} on: {}", coordinates, member);
            return ksCoords.get(coordinates);
        }
    }
}
