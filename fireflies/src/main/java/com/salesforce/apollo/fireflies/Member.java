/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.X509CertificateHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avro.AccusationDigest;
import com.salesforce.apollo.avro.CertificateDigest;
import com.salesforce.apollo.avro.EncodedCertificate;
import com.salesforce.apollo.avro.NoteDigest;
import com.salesforce.apollo.avro.Signed;
import com.salesforce.apollo.avro.Uuid;
import com.salesforce.apollo.fireflies.View.AccTag;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * A member of the view
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Member implements Comparable<Member> {

    public static final String PORT_TEMPLATE = "%s:%s:%s";
    public static final String RING_HASH_ALGORITHM = Conversion.SHA_256;
    private static final Logger log = LoggerFactory.getLogger(Member.class);
    private static final String PORT_SEPARATOR = ":";
    private static final String RING_HASH_TEMPLATE = "%s-gossip-%s";

    public static UUID getMemberId(X509Certificate c) {
        X509CertificateHolder holder;
        try {
            holder = new X509CertificateHolder(c.getEncoded());
        } catch (CertificateEncodingException | IOException e) {
            throw new IllegalArgumentException("invalid identity certificate for member: " + c, e);
        }
        ByteBuffer buf = ByteBuffer.wrap(holder.getExtension(Extension.subjectKeyIdentifier)
                                               .getExtnValue()
                                               .getOctets());
        UUID memberId = new UUID(buf.getLong(), buf.getLong());
        return memberId;
    }

    /**
     * @param certificate
     * @return array of 3 InetSocketAddress in the ordering of {fireflies, ghost, avalanche)
     */
    public static InetSocketAddress[] portsFrom(X509Certificate certificate) {

        String dn = certificate.getSubjectX500Principal().getName();
        Map<String, String> decoded = Util.decodeDN(dn);
        String portString = decoded.get("L");
        if (portString == null) {
            throw new IllegalArgumentException("Invalid certificate, no port encodings in \"L\" of dn= " + dn);
        }
        String[] ports = portString.split(PORT_SEPARATOR);
        if (ports.length != 3) {
            throw new IllegalArgumentException("Invalid port encodings (not == 3 ports) in \"L\" of dn= " + dn);
        }
        int ffPort = Integer.parseInt(ports[0]);
        int gPort = Integer.parseInt(ports[1]);
        int aPort = Integer.parseInt(ports[2]);

        String hostName = decoded.get("CN");
        if (hostName == null) {
            throw new IllegalArgumentException("Invalid certificate, missing \"CN\" of dn= " + dn);
        }
        return new InetSocketAddress[] { new InetSocketAddress(hostName, ffPort),
                new InetSocketAddress(hostName, gPort), new InetSocketAddress(hostName, aPort) };
    }

    public static Uuid uuidBits(UUID id) {
        return Conversion.uuidBits(id);
    }

    /**
     * The member's latest note
     */
    volatile Note note;

    /**
     * The valid accusatons for this member
     */
    final Map<Integer, Accusation> validAccusations = new ConcurrentHashMap<>();

    /**
     * Avalanche socket endpoint for the member
     */
    private final InetSocketAddress avalancheEndpoint;

    /**
     * Signing identity
     */
    private final X509Certificate certificate;

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

    /**
     * Fireflies socket endpoint for the member
     */
    private final InetSocketAddress firefliesEndpoint;

    /**
     * Ghost DHT socket endpoint for the member
     */
    private final InetSocketAddress ghostEndpoint;

    /**
     * Unique ID of the memmber
     */
    private final UUID id;

    /**
     * Precomputed hashes for all the configured rings
     */
    private final HashKey[] ringHashes;

    public Member(X509Certificate c, byte[] derEncodedCertificate, FirefliesParameters parameters,
            byte[] certificateHash) {
        this(c, derEncodedCertificate, parameters, certificateHash, portsFrom(c));
    }

    public Member(X509Certificate c, FirefliesParameters parameters) {
        this(c, null, parameters, null);

    }

    protected Member(X509Certificate c, byte[] derEncodedCertificate, FirefliesParameters parameters,
            byte[] certificateHash, InetSocketAddress[] boundPorts) {
        assert c != null;
        certificate = c;
        id = getMemberId(c);
        if (derEncodedCertificate != null) {
            this.derEncodedCertificate = derEncodedCertificate;
        } else {
            try {
                this.derEncodedCertificate = certificate.getEncoded();
            } catch (CertificateEncodingException e) {
                throw new IllegalArgumentException("Cannot encode certifiate for member: " + id, e);
            }
        }

        firefliesEndpoint = boundPorts[0];
        ghostEndpoint = boundPorts[1];
        avalancheEndpoint = boundPorts[2];
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
                derEncodedCertificate = certificate.getEncoded();
            } catch (CertificateEncodingException e) {
                throw new IllegalArgumentException("Cannot encode certifiate for member: " + id, e);
            }

            md.update(derEncodedCertificate);
            this.certificateHash = md.digest();
        }
        ringHashes = new HashKey[parameters.rings];
        MessageDigest md;
        try {
            md = MessageDigest.getInstance(Conversion.SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("No hash algorithm found: " + parameters.hashAlgorithm);
        }
        for (int ring = 0; ring < parameters.rings; ring++) {
            md.reset();
            md.update(String.format(RING_HASH_TEMPLATE, id, ring).getBytes());
            ringHashes[ring] = new HashKey(md.digest());
        }
    }

    @Override
    public int compareTo(Member o) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Member))
            return false;
        Member other = (Member)obj;
        return id.equals(other.id);
    }

    public InetSocketAddress getAvalancheEndpoint() {
        return avalancheEndpoint;
    }

    /**
     * @return the identifying certificate of the member
     */
    public X509Certificate getCertificate() {
        return certificate;
    }

    /**
     * @return the Instant this member was determined as failing
     */
    public Instant getFailedAt() {
        return failedAt;
    }

    public InetSocketAddress getFirefliesEndpoint() {
        return firefliesEndpoint;
    }

    public InetSocketAddress getGhostEndpoint() {
        return ghostEndpoint;
    }

    /**
     * @return the host address of the member
     */
    public InetAddress getHost() {
        return firefliesEndpoint.getAddress();
    }

    /**
     * @return the unique id of this member
     */
    public UUID getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public HashKey hashFor(int ring) {
        assert ring < ringHashes.length && ring >= 0 : "Invalid ring";
        return ringHashes[ring];
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
        return "Member[" + id + "]";
    }

    /**
     * Add an accusation to the member
     * 
     * @param accusation
     */
    void addAccusation(Accusation accusation) {
        Note n = getNote();
        if (n == null) { return; }
        Integer ringNumber = accusation.getRingNumber();
        if (n.getEpoch() != accusation.getEpoch()) {
            log.trace("Invalid epoch discarding accusation from {} on {} ring {}", accusation.getAccuser(), id,
                      ringNumber);
        }
        if (n.getMask().get(ringNumber)) {
            validAccusations.put(ringNumber, accusation);
            if (log.isDebugEnabled()) {
                log.debug("Member {} is accusing {} on {}", accusation.getAccuser(), id, ringNumber);
            }
        }
    }

    /**
     * clear all accusations for the member
     */
    void clearAccusations() {
        validAccusations.clear();
        log.trace("Clearing accusations for {}", id);
    }

    /**
     * Answer the Signature, initialized with the member's public key, using the supplied signature algorithm.
     * 
     * @param signatureAlgorithm
     * @return the signature, initialized for verification
     */
    Signature forVerification(String signatureAlgorithm) {
        Signature signature;
        try {
            signature = Signature.getInstance(signatureAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + signatureAlgorithm, e);
        }
        try {
            signature.initVerify(certificate.getPublicKey());
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid public key", e);
        }
        return signature;
    }

    Accusation getAccusation(int index) {
        return validAccusations.get(index);
    }

    Stream<AccusationDigest> getAccusationDigests() {
        return validAccusations.values()
                               .stream()
                               .map(e -> new AccusationDigest(uuidBits(id), e.getEpoch(), e.getRingNumber()));
    }

    List<AccTag> getAccusationTags() {
        return validAccusations.keySet().stream().map(ring -> new AccTag(id, ring)).collect(Collectors.toList());
    }

    CertificateDigest getCertificateDigest() {
        Note current = note;
        return current == null ? null
                : new CertificateDigest(uuidBits(id), getEpoch(), ByteBuffer.wrap(certificateHash));
    }

    byte[] getCertificateHash() {
        return certificateHash;
    }

    Signed getEncodedAccusation(Integer ring) {
        Accusation accusation = validAccusations.get(ring);
        return accusation == null ? null : accusation.getSigned();
    }

    List<Signed> getEncodedAccusations() {
        return IntStream.range(0, ringHashes.length)
                        .mapToObj(i -> getEncodedAccusation(i))
                        .filter(e -> e != null)
                        .collect(Collectors.toList());
    }

    EncodedCertificate getEncodedCertificate() {
        CertificateDigest digest = getCertificateDigest();
        return digest == null ? null : new EncodedCertificate(digest, ByteBuffer.wrap(derEncodedCertificate));
    }

    long getEpoch() {
        Note current = note;
        if (current == null) { return 0; }
        return current.getEpoch();
    }

    Note getNote() {
        return note;
    }

    NoteDigest getNoteDigest() {
        Note current = note;
        return current == null ? null : new NoteDigest(uuidBits(id), current.getEpoch());
    }

    Signed getSignedNote() {
        Note current = note;
        return current == null ? null : current.getSigned();
    }

    void invalidateAccusationOnRing(int index) {
        validAccusations.remove(index);
        log.trace("Invalidating accusations of {} on {}", id, index);
    }

    boolean isAccused() {
        return !validAccusations.isEmpty();
    }

    boolean isAccusedOn(int index) {
        return validAccusations.containsKey(index);
    }

    void reset() {
        failedAt = null;
        validAccusations.clear();
        note = null;
        log.trace("Reset {}", id);
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
                log.info("discarding note for {} with wrong previous epoch {} : {}" + id, nextEpoch, currentEpoch);
                return;
            }
        }
        this.note = next;
        clearAccusations();
        failedAt = null;
    }
}
