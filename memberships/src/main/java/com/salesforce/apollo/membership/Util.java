/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

/**
 * @author hal.hildebrand
 * @since 220
 */
final public class Util {

    /**
     * A single event X has probability p. Calculate probability of k occurrences X
     * within n events
     * 
     * @param k - number of occurrences
     * @param n - number of events
     * @param p - probability of event occurring
     * @return probability of k occurrences X within n events
     */
    public static double binomialc(int k, int n, double p) {
        // special cases
        if (p == 0.0) {
            return 1.0;
        }
        if (p == 1.0) {
            if (k == n) {
                return 1.0;
            } else {
                return 0.0;
            }
        }

        double q = 1.0 - p;
        double m = n + 1;
        double r = Math.log(p / q);
        double prob = n * Math.log(q);
        double cum = Math.exp(prob);
        for (int i = 1; i <= k + 1; i++) {
            prob += r + Math.log(m / i - 1.0);
            cum += Math.exp(prob);
        }
        return cum;
    }

    public static UUID decode(String encoded) {
        ByteBuffer bb = ByteBuffer.wrap(Base64.getUrlDecoder().decode(encoded));
        return new UUID(bb.getLong(), bb.getLong());
    }

    public static Map<String, String> decodeDN(String dn) {
        LdapName ldapDN;
        try {
            ldapDN = new LdapName(dn);
        } catch (InvalidNameException e) {
            throw new IllegalArgumentException("invalid DN: " + dn, e);
        }
        Map<String, String> decoded = new HashMap<>();
        ldapDN.getRdns().forEach(rdn -> {
            Object value = rdn.getValue();
            try {
                decoded.put(rdn.getType(), (String) value);
            } catch (ClassCastException e) {
                // skip
            }
        });
        return decoded;
    }

    public static String encode(UUID uuid) {
        byte[] bytes = ByteBuffer.allocate(16)
                                 .putLong(uuid.getMostSignificantBits())
                                 .putLong(uuid.getLeastSignificantBits())
                                 .array();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    public static int minMajority(double pByz, int size, int e) {
        double pTarget = ((double) e) / ((double) size);
        for (int t = 1; t <= 10000; t++) {
            double pf = 1.0 - binomialc(t, 2 * t + 1, pByz);
            if (pTarget >= pf) {
                return t;
            }
        }
        throw new IllegalArgumentException("Cannot compute number if rings from pByz=" + pByz);
    }

    public static byte[] sign(byte[] attestation, Signature signature) {
        try {
            signature.update(attestation);
            return signature.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Error during signing of " + attestation, e);
        }
    }

    public static String toHex(byte[] bytes) {
        BigInteger bi = new BigInteger(1, bytes);
        return String.format("%0" + (bytes.length << 1) + "X", bi);
    }

    private Util() {
    }
}
