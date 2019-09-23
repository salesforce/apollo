/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.salesforce.apollo.protocols.Conversion;

/**
 * Parameters defining the operation of Fireflies
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class FirefliesParameters {

    public static final String DEFAULT_HASH_ALGORITHM = Conversion.SHA_256;
    public static final String DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";

    /**
     * The maximum allowed size of the message buffer
     */
    public int bufferSize = 400000;

    /**
     * The CA certificate that signs all the member's certificates
     */
    public final X509Certificate ca;

    /**
     * The maximum cardinality of the fireflies group
     */
    public final int cardinality;

    /**
     * Secure source of randomness
     */
    @JsonIgnore
    public final SecureRandom entropy;

    /**
     * The percentage of the members (cardinality) that we can tolerate from a failure or byzantine subversion
     */
    public final double faultToleranceLevel;

    /**
     * The JCE algorithm name used for secure hashes
     */
    public final String hashAlgorithm;
    /**
     * The probability of a member being subversive
     */
    public final double probabilityByzantine;
    /**
     * The number of rings based on the parameters required to resist failure in gossip and byzantine members.
     */
    public final int rings;
    /**
     * The JCE algorithm name used for signatures
     */
    public final String signatureAlgorithm;
    /**
     * The number of rings tolerated either by a failure or through byzantine subversion
     */
    public final int toleranceLevel;

    public FirefliesParameters(X509Certificate ca) {
        this(ca, new SecureRandom(), DEFAULT_SIGNATURE_ALGORITHM, DEFAULT_HASH_ALGORITHM);
    }

    public FirefliesParameters(X509Certificate ca, SecureRandom entropy, String signatureAlgorithm, String hashAlgorithm) {
        this.ca = ca;
        this.signatureAlgorithm = signatureAlgorithm;
        this.entropy = entropy;
        this.hashAlgorithm = hashAlgorithm;

        String dn = ca.getSubjectX500Principal().getName();
        Map<String, String> decoded = Util.decodeDN(dn);
        String encoded = decoded.get("O");
        if (encoded == null) {
            throw new IllegalArgumentException("No \"O\" in dn: " + dn);
        }
        String[] split = encoded.split(":");
        if (split.length != 3) {
            throw new IllegalArgumentException("Invalid format of organization: " + encoded);
        }

        cardinality = Integer.parseInt(split[0]);
        faultToleranceLevel = Double.parseDouble(split[1]);
        probabilityByzantine = Double.parseDouble(split[2]);
        toleranceLevel = Util.minMajority(probabilityByzantine, faultToleranceLevel);
        rings = toleranceLevel * 2 + 1;
    }
}
