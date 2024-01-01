package com.salesforce.apollo.fireflies;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.HashMultiset;
import com.salesforce.apollo.archipelago.RouterImpl;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Verifiers that delegate to a majority of the sample for event validation and verification
 * </p>
 *
 * @author hal.hildebrand
 **/
public class Bootstrapper implements Verifiers {
    private final static Logger                                       log = LoggerFactory.getLogger(Bootstrapper.class);
    private final        List<? extends Member>                       successors;
    private final        SigningMember                                member;
    private final        int                                          majority;
    private final        LoadingCache<IdentifierSequence, KeyState>   ksSeq;
    private final        RouterImpl.CommonCommunications<Entrance, ?> communications;
    private final        Duration                                     operationTimeout;
    private final        Duration                                     operationsFrequency;

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
        ksSeq = Caffeine.newBuilder()
                        .maximumSize(100)
                        .expireAfterWrite(Duration.ofMinutes(1))
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
            public KeyState keyState(Identifier identifier, ULong seqNum) {
                log.trace("Get key state: {}:{} on: {}", identifier, seqNum, member.getId());
                return ksSeq.get(new IdentifierSequence(identifier, seqNum));
            }

            @Override
            public boolean validate(EstablishmentEvent event) {
                log.trace("Validate event: {} on: {}", event, member.getId());
                return validate(event.getCoordinates());
            }

            @Override
            public boolean validate(EventCoordinates coordinates) {
                log.trace("Validating coordinates: {} on: {}", coordinates, member.getId());
                return Bootstrapper.this.validate(coordinates);
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

    private KeyState delegate(IdentifierSequence idSeq) {
        log.info("Get key state: {} from slice on: {}", idSeq, member.getId());
        var iterator = new SliceIterator<>("Retrieve KeyState", member, successors, communications);
        final var identifierSeq = idSeq.toIdSeq();
        var ks = new CompletableFuture<KeyState>();
        HashMultiset<KeyState_> keystates = HashMultiset.create();
        iterator.iterate((link, m) -> {
            log.debug("Requesting Key State from: {} on: {}", link.getMember().getId(), member.getId());
            return link.keyState(identifierSeq);
        }, (futureSailor, link, m) -> complete(ks, futureSailor, keystates, m), () -> {
            if (!ks.isDone()) {
                log.warn("Failed to retrieve key state: {} from slice on: {}", idSeq, member.getId());
                ks.complete(null);
            }
        }, Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()), operationsFrequency);
        try {
            return ks.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("Unable to retrieve key state: {} on: {}", idSeq, member.getId());
        }
        return null;
    }

    private boolean validate(EventCoordinates coordinates) {
        log.info("Validate event: {} from slice on: {}", coordinates, member.getId());
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
                log.warn("Failed to validate: {} from slice on: {}", coordinates, member.getId());
                valid.complete(Validation.newBuilder().setResult(false).build());
            }
        }, Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()), operationsFrequency);
        try {
            return valid.get().getResult();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("Unable to validate: {} on: {}", coordinates, member.getId());
        }
        return false;
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
            log.info("Get key state: {} on: {}", key, member.getId());
            return ksSeq.get(key);
        }
    }
}
