/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.membership;

import static com.salesforce.apollo.crypto.QualifiedBase64.*;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.Map;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.cert.BcX500NameDnImpl;
import com.salesforce.apollo.membership.Util;
import com.salesforce.apollo.utils.Hex;

/**
 * A member of the view
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Member implements Comparable<Member> {

    public static BcX500NameDnImpl encode(Digest digest, String host, int port, PublicKey signingKey) {
        return new BcX500NameDnImpl(String.format("CN=%s, L=%s, UID=%s, SN=%s", host, port, qb64(digest), qb64(signingKey)));
    }

    public static Digest getMemberId(X509Certificate cert) {
        String dn = cert.getSubjectX500Principal().getName();
        Map<String, String> decoded = Util.decodeDN(dn);
        String id = decoded.get("UID");
        if (id == null) {
            throw new IllegalArgumentException("Invalid certificate, missing \"UID\" of dn= " + dn);
        }
        return digest(id);
    }

    /**
     * @param certificate
     * @return host and port for the member indicated by the certificate
     */
    public static InetSocketAddress portsFrom(X509Certificate certificate) {

        String dn = certificate.getSubjectX500Principal().getName();
        Map<String, String> decoded = Util.decodeDN(dn);
        String portString = decoded.get("L");
        if (portString == null) {
            throw new IllegalArgumentException("Invalid certificate, no port encodings in \"L\" of dn= " + dn);
        }
        int port = Integer.parseInt(portString);

        String hostName = decoded.get("CN");
        if (hostName == null) {
            throw new IllegalArgumentException("Invalid certificate, missing \"CN\" of dn= " + dn);
        }
        return new InetSocketAddress(hostName, port);
    }

    private static PublicKey getSigningKey(X509Certificate cert) {
        String dn = cert.getSubjectX500Principal().getName();
        Map<String, String> decoded = Util.decodeDN(dn);
        String pk = decoded.get("SN");
        if (pk == null) {
            throw new IllegalArgumentException("Invalid certificate, missing \"SN\" of dn= " + dn);
        }
        return publicKey(pk);
    }

    /**
     * Signing identity
     */
    private final X509Certificate    certificate;
    /**
     * Unique ID of the memmber
     */
    private final Digest             id;
    /**
     * cached signature algorithm for signing key
     */
    private final SignatureAlgorithm signatureAlgorithm;
    /**
     * Key used by member to sign things
     */
    private final PublicKey          signingKey;

    public Member(Digest id, X509Certificate c, PublicKey sk) {
        certificate = c;
        this.id = id;
        this.signingKey = sk;
        signatureAlgorithm = SignatureAlgorithm.lookup(signingKey);
    }

    public Member(X509Certificate cert) {
        this(getMemberId(cert), cert, getSigningKey(cert));
    }

    @Override
    public int compareTo(Member o) {
        return id.compareTo(o.getId());
    }

    @Override
    // The id of a member uniquely identifies it
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof Member))
            return false;
        return id.equals(((Member) obj).id);
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
    public Digest getId() {
        return id;
    }

    public SignatureAlgorithm getSignatureAlgorithm() {
        return signatureAlgorithm;
    }

    public PublicKey getSigningKey() {
        return signingKey;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Member[" + Hex.hex(id.getBytes()) + "]";
    }

    /**
     * Verify the signature with the member's signing key
     */
    public boolean verify(byte[] message, JohnHancock signature) {
        return signatureAlgorithm.verify(message, signature, signingKey);
    }

}
