/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AccusationDigest;
import com.salesfoce.apollo.proto.CertificateDigest;
import com.salesfoce.apollo.proto.EncodedCertificate;
import com.salesfoce.apollo.proto.NoteDigest;
import com.salesfoce.apollo.proto.Signed;
import com.salesforce.apollo.fireflies.View.AccTag;
import com.salesforce.apollo.membership.Member;

/**
 * A member of the view
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Participant extends Member {
    private static final Logger log = LoggerFactory.getLogger(Participant.class);

    /**
     * The member's latest note
     */
    volatile Note note;

    /**
     * The valid accusatons for this member
     */
    final Map<Integer, Accusation> validAccusations = new ConcurrentHashMap<>();

    /**
     * The hash of the member's certificate
     */
    private final byte[] certificateHash;

    /**
     * The DER serialized certificate
     */
    private final byte[] derEncodedCertificate;

    /**
     * Instant when a member failed, null if not failed
     */
    private volatile Instant failedAt = Instant.now();

    public Participant(X509Certificate c, FirefliesParameters parameters) {
        this(c, null, parameters, null);

    }

    protected Participant(X509Certificate c, byte[] derEncodedCertificate, FirefliesParameters parameters,
            byte[] certificateHash) {
        super(getMemberId(c), c);
        assert c != null;
        if (derEncodedCertificate != null) {
            this.derEncodedCertificate = derEncodedCertificate;
        } else {
            try {
                this.derEncodedCertificate = getCertificate().getEncoded();
            } catch (CertificateEncodingException e) {
                throw new IllegalArgumentException("Cannot encode certifiate for member: " + getId(), e);
            }
        }

        if (certificateHash != null) {
            this.certificateHash = certificateHash;
        } else {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance(parameters.hashAlgorithm);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("No hash algorithm found: " + parameters.hashAlgorithm);
            }
            try {
                derEncodedCertificate = getCertificate().getEncoded();
            } catch (CertificateEncodingException e) {
                throw new IllegalArgumentException("Cannot encode certifiate for member: " + getId(), e);
            }

            md.update(derEncodedCertificate);
            this.certificateHash = md.digest();
        }
    }

    /**
     * @return the Instant this member was determined as failing
     */
    public Instant getFailedAt() {
        return failedAt;
    }

    public boolean isFailed() {
        Instant current = failedAt;
        return current != null;
    }

    public boolean isLive() {
        return !isFailed();
    }

    @Override
    public String toString() {
        return "Member[" + getId() + "]";
    }

    /**
     * Add an accusation to the member
     * 
     * @param accusation
     */
    void addAccusation(Accusation accusation) {
        Note n = getNote();
        if (n == null) {
            return;
        }
        Integer ringNumber = accusation.getRingNumber();
        if (n.getEpoch() != accusation.getEpoch()) {
            log.trace("Invalid epoch discarding accusation from {} on {} ring {}", accusation.getAccuser(), getId(),
                      ringNumber);
        }
        if (n.getMask().get(ringNumber)) {
            validAccusations.put(ringNumber, accusation);
            if (log.isDebugEnabled()) {
                log.debug("Member {} is accusing {} on {}", accusation.getAccuser(), getId(), ringNumber);
            }
        }
    }

    /**
     * clear all accusations for the member
     */
    void clearAccusations() {
        validAccusations.clear();
        log.trace("Clearing accusations for {}", getId());
    }

    Accusation getAccusation(int index) {
        return validAccusations.get(index);
    }

    Stream<AccusationDigest> getAccusationDigests() {
        return validAccusations.values()
                               .stream()
                               .map(e -> AccusationDigest.newBuilder()
                                                         .setId(getId().toByteString())
                                                         .setEpoch(e.getEpoch())
                                                         .setRing(e.getRingNumber())
                                                         .build());
    }

    List<AccTag> getAccusationTags() {
        return validAccusations.keySet().stream().map(ring -> new AccTag(getId(), ring)).collect(Collectors.toList());
    }

    CertificateDigest getCertificateDigest() {
        Note current = note;
        return current == null ? null
                : CertificateDigest.newBuilder()
                                   .setId(getId().toByteString())
                                   .setEpoch(current.getEpoch())
                                   .setHash(ByteString.copyFrom(certificateHash))
                                   .build();
    }

    byte[] getCertificateHash() {
        return certificateHash;
    }

    Signed getEncodedAccusation(Integer ring) {
        Accusation accusation = validAccusations.get(ring);
        return accusation == null ? null : accusation.getSigned();
    }

    List<Signed> getEncodedAccusations(int rings) {
        return IntStream.range(0, rings)
                        .mapToObj(i -> getEncodedAccusation(i))
                        .filter(e -> e != null)
                        .collect(Collectors.toList());
    }

    EncodedCertificate getEncodedCertificate() {
        CertificateDigest digest = getCertificateDigest();
        return digest == null ? null
                : EncodedCertificate.newBuilder()
                                    .setDigest(digest)
                                    .setContent(ByteString.copyFrom(derEncodedCertificate))
                                    .build();
    }

    long getEpoch() {
        Note current = note;
        if (current == null) {
            return 0;
        }
        return current.getEpoch();
    }

    Note getNote() {
        return note;
    }

    NoteDigest getNoteDigest() {
        Note current = note;
        return current == null ? null
                : NoteDigest.newBuilder().setId(getId().toByteString()).setEpoch(current.getEpoch()).build();
    }

    Signed getSignedNote() {
        Note current = note;
        return current == null ? null : current.getSigned();
    }

    void invalidateAccusationOnRing(int index) {
        validAccusations.remove(index);
        log.trace("Invalidating accusations of {} on {}", getId(), index);
    }

    boolean isAccused() {
        return !validAccusations.isEmpty();
    }

    boolean isAccusedOn(int index) {
        return validAccusations.containsKey(index);
    }

    void reset() {
        failedAt = Instant.now();
        note = null;
        validAccusations.clear();
        log.trace("Reset {}", getId());
    }

    void setFailed(boolean failed) {
        this.failedAt = failed ? Instant.now() : null;
    }

    void setNote(Note next) {
        Note current = this.note;
        if (current != null) {
            long nextEpoch = next.getEpoch();
            long currentEpoch = current.getEpoch();
            if (currentEpoch < nextEpoch - 1) {
                log.info("discarding note for {} with wrong previous epoch {} : {}" + getId(), nextEpoch, currentEpoch);
                return;
            }
        }
        this.note = next;
        failedAt = null;
        clearAccusations();
    }
}
