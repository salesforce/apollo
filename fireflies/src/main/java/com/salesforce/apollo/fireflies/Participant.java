/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.io.InputStream;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.fireflies.proto.EncodedCertificate;
import com.salesfoce.apollo.fireflies.proto.SignedAccusation;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.fireflies.View.AccTag;
import com.salesforce.apollo.membership.Member;

/**
 * A member of the view
 *
 * @author hal.hildebrand
 * @since 220
 */
public class Participant implements Member {
    private static final Logger log = LoggerFactory.getLogger(Participant.class);

    /**
     * Certificate
     */
    protected volatile X509Certificate                  certificate;
    /**
     * The hash of the member's certificate
     */
    protected final Digest                              certificateHash;
    /**
     * The DER serialized certificate
     */
    protected final byte[]                              derEncodedCertificate;
    protected final AtomicReference<EncodedCertificate> encoded = new AtomicReference<>();
    protected final DigestAlgorithm                     hashAlgorithm;
    /**
     * The member's latest note
     */
    protected volatile NoteWrapper                      note;

    /**
     * The valid accusatons for this member
     */
    protected final Map<Integer, AccusationWrapper> validAccusations = new ConcurrentHashMap<>();
    private final Member                            wrapped;

    public Participant(Member wrapped, FirefliesParameters parameters) {
        this(wrapped, wrapped.getCertificate(), parameters);
    }

    public Participant(Member wrapped, X509Certificate certificate, FirefliesParameters parameters) {
        assert wrapped != null;
        this.wrapped = wrapped;
        this.hashAlgorithm = parameters.hashAlgorithm;
        this.certificate = certificate;
        try {
            this.derEncodedCertificate = getCertificate().getEncoded();
        } catch (CertificateEncodingException e) {
            throw new IllegalArgumentException("Cannot encode certifiate for member: " + getId(), e);
        }
        this.certificateHash = DigestAlgorithm.DEFAULT.digest(derEncodedCertificate);
    }

    @Override
    public int compareTo(Member o) {
        return wrapped.compareTo(o);
    }

    @Override
    public boolean equals(Object obj) {
        return wrapped.equals(obj);
    }

    @Override
    public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return wrapped.filtered(threshold, signature, message);
    }

    @Override
    public X509Certificate getCertificate() {
        return certificate;
    }

    @Override
    public Digest getId() {
        return wrapped.getId();
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public String toString() {
        return "Member[" + getId() + "]";
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        return wrapped.verify(signature, message);
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return wrapped.verify(threshold, signature, message);
    }

    /**
     * Add an accusation to the member
     *
     * @param accusation
     */
    void addAccusation(AccusationWrapper accusation) {
        NoteWrapper n = getNote();
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

    AccusationWrapper getAccusation(int index) {
        return validAccusations.get(index);
    }

    Stream<AccusationWrapper> getAccusations() {
        return validAccusations.values().stream();
    }

    List<AccTag> getAccusationTags() {
        return validAccusations.keySet().stream().map(ring -> new AccTag(getId(), ring)).collect(Collectors.toList());
    }

    Digest getCertificateHash() {
        return certificateHash;
    }

    AccusationWrapper getEncodedAccusation(Integer ring) {
        return validAccusations.get(ring);
    }

    List<SignedAccusation> getEncodedAccusations(int rings) {
        return IntStream.range(0, rings)
                        .mapToObj(i -> getEncodedAccusation(i))
                        .filter(e -> e != null)
                        .map(e -> e.getWrapped())
                        .collect(Collectors.toList());
    }

    EncodedCertificate getEncodedCertificate() {
        return encoded.get();
    }

    long getEpoch() {
        NoteWrapper current = note;
        if (current == null) {
            return 0;
        }
        return current.getEpoch();
    }

    NoteWrapper getNote() {
        return note;
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
        note = null;
        encoded.set(null);
        validAccusations.clear();
        log.trace("Reset {}", getId());
    }

    void setNote(NoteWrapper next) {
        NoteWrapper current = note;
        if (current != null) {
            long nextEpoch = next.getEpoch();
            long currentEpoch = current.getEpoch();
            if (currentEpoch > 0 && currentEpoch < nextEpoch - 1) {
                log.info("discarding note for {} with wrong previous epoch {} : {}", getId(), nextEpoch, currentEpoch);
                return;
            }
        }
        note = next;
        encoded.set(EncodedCertificate.newBuilder()
                                      .setId(getId().toDigeste())
                                      .setEpoch(note.getEpoch())
                                      .setHash(certificateHash.toDigeste())
                                      .setContent(ByteString.copyFrom(derEncodedCertificate))
                                      .build());
        clearAccusations();
    }
}
