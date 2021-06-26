/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * @author hal.hildebrand
 *
 */
public interface Member extends Comparable<Member> {

    static Digest getMemberIdentifier(X509Certificate cert) {
        String dn = cert.getSubjectX500Principal().getName();
        Map<String, String> decoded = Util.decodeDN(dn);
        String id = decoded.get("UID");
        if (id == null) {
            throw new IllegalArgumentException("Invalid certificate, missing \"UID\" of dn= " + dn);
        }
        return digest(id);
    }

    static PublicKey getSigningKey(X509Certificate cert) {
        String dn = cert.getSubjectX500Principal().getName();
        Map<String, String> decoded = Util.decodeDN(dn);
        String pk = decoded.get("DC");
        if (pk == null) {
            throw new IllegalArgumentException("Invalid certificate, missing \"DC\" of dn= " + dn);
        }
        return publicKey(pk);
    }

    /**
     * @param certificate
     * @return host and port for the member indicated by the certificate
     */
    static InetSocketAddress portsFrom(X509Certificate certificate) {

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

    @Override
    int compareTo(Member o);

    // The id of a member uniquely identifies it
    @Override
    boolean equals(Object obj);

    /**
     * @return the identifying certificate of the member
     */
    X509Certificate getCertificate();

    /**
     * @return the unique id of this member
     */
    Digest getId();

    default SelfAddressingIdentifier getIdentifier() {
        return new SelfAddressingIdentifier(getId());
    }

    @Override
    int hashCode();

    /**
     * Verify the signature with the member's signing key
     */
    default boolean verify(JohnHancock signature, byte[]... message) {
        return verify(signature, BbBackedInputStream.aggregate(message));
    }

    default boolean verify(JohnHancock signature, ByteBuffer... message) {
        return verify(signature, BbBackedInputStream.aggregate(message));
    }

    default boolean verify(JohnHancock signature, ByteString... message) {
        return verify(signature, BbBackedInputStream.aggregate(message));
    }

    boolean verify(JohnHancock signature, InputStream message);

    default boolean verify(JohnHancock sig, List<ByteBuffer> buffers) {
        return verify(sig, BbBackedInputStream.aggregate(buffers));
    }

}
