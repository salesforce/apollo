/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.security.PublicKey;
import java.util.List;
import java.util.stream.Stream;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.QualifiedBase64;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.SigningThreshold.Weighted.Weight;
import com.salesforce.apollo.utils.Hex;

/**
 * @author hal.hildebrand
 *
 */
public class KeyConfigurationDigester {

    public static Digest digest(SigningThreshold signingThreshold, List<Digest> nextKeyDigests) {
        var st = signingThresholdRepresentation(signingThreshold);
        var digestAlgorithm = nextKeyDigests.get(0).getAlgorithm();

        var digest = digestAlgorithm.digest(st).getBytes();// digest
        for (var d : nextKeyDigests) {
            var keyDigest = d.getBytes();
            for (var i = keyDigest.length - 1; i >= 0; i--) {
                digest[i] = (byte) (digest[i] ^ keyDigest[i]);
            }
        }

        return new Digest(nextKeyDigests.get(0).getAlgorithm(), digest);
    }

    public static Digest digest(SigningThreshold signingThreshold, List<PublicKey> nextKeys, DigestAlgorithm algo) {
        var keyDigs = nextKeys.stream()
                              .map(QualifiedBase64::qb64)
                              .map(qb64 -> qb64.getBytes(UTF_8))
                              .map(algo::digest)
                              .collect(toList());

        return digest(signingThreshold, keyDigs);
    }

    public static boolean matches(SigningThreshold signingThreshold, List<PublicKey> nextKeys, Digest in) {
        return digest(signingThreshold, nextKeys, in.getAlgorithm()).equals(in);
    }

    static byte[] signingThresholdRepresentation(SigningThreshold threshold) {
        if (threshold instanceof SigningThreshold.Unweighted) {
            return Hex.hexNoPad(((SigningThreshold.Unweighted) threshold).threshold()).getBytes(UTF_8);
        } else if (threshold instanceof SigningThreshold.Weighted) {
            return Stream.of(((SigningThreshold.Weighted) threshold).weights())
                         .map(lw -> Stream.of(lw).map(KeyConfigurationDigester::weight).collect(joining(",")))
                         .collect(joining(("&")))
                         .getBytes(UTF_8);
        } else {
            throw new IllegalArgumentException("Unknown threshold type: " + threshold.getClass());
        }
    }

    static String weight(Weight w) {
        if (w.denominator().isEmpty()) {
            return "" + w.numerator();
        }

        return w.numerator() + "/" + w.denominator().get();
    }

}
