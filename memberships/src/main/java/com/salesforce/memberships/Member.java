/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.memberships;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.UUID;

import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.X509CertificateHolder;

import com.salesforce.apollo.avro.Uuid;
import com.salesforce.apollo.protocols.Conversion;

/**
 * A member of the view
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Member implements Comparable<Member> {

    public static final String  PORT_TEMPLATE  = "%s:%s:%s";
    private static final String PORT_SEPARATOR = ":";

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
     * @return array of 3 InetSocketAddress in the ordering of {fireflies, ghost,
     *         avalanche)
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
                                         new InetSocketAddress(hostName, gPort),
                                         new InetSocketAddress(hostName, aPort) };
    }

    public static Uuid uuidBits(UUID id) {
        return Conversion.uuidBits(id);
    }

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

    public byte[] getCertificateHash() {
        return certificateHash;
    }

    public byte[] getDerEncodedCertificate() {
        return derEncodedCertificate;
    }

    /**
     * Unique ID of the memmber
     */
    private final UUID id;

    public Member(X509Certificate c, byte[] derEncodedCertificate, byte[] certificateHash) {
        this(c, derEncodedCertificate, certificateHash, portsFrom(c));
    }

    public Member(X509Certificate c) {
        this(c, null, null);

    }

    protected Member(X509Certificate c, byte[] derEncodedCertificate, byte[] certificateHash,
            InetSocketAddress[] boundPorts) {
        assert c != null;
        certificate = c;
        id = getMemberId(c);
        this.derEncodedCertificate = derEncodedCertificate;
        this.certificateHash = certificateHash;
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
        Member other = (Member) obj;
        return id.equals(other.id);
    }

    /**
     * @return the identifying certificate of the member
     */
    public X509Certificate getCertificate() {
        return certificate;
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

    @Override
    public String toString() {
        return "Member[" + id + "]";
    }

    /**
     * Answer the Signature, initialized with the member's public key, using the
     * supplied signature algorithm.
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

}
