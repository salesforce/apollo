/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier.spec;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.SigningThreshold.Weighted.Weight;
import com.salesforce.apollo.utils.Hex;

import java.security.PublicKey;
import java.util.List;
import java.util.stream.Stream;

import static com.salesforce.apollo.cryptography.QualifiedBase64.bs;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * @author hal.hildebrand
 */
public class KeyConfigurationDigester {

    public static Digest digest(SigningThreshold signingThreshold, List<Digest> nextKeyDigests) {
        var st = signingThresholdRepresentation(signingThreshold);
        var digestAlgorithm = nextKeyDigests.getFirst().getAlgorithm();

        var digest = digestAlgorithm.digest(st);// digest

        for (var d : nextKeyDigests) {
            digest = digest.xor(d);
        }

        return digest;
    }

    public static Digest digest(SigningThreshold signingThreshold, List<PublicKey> nextKeys, DigestAlgorithm algo) {
        var keyDigs = nextKeys.stream().map(k -> bs(k).toByteString()).map(algo::digest).collect(toList());

        return digest(signingThreshold, keyDigs);
    }

    public static boolean matches(SigningThreshold signingThreshold, List<PublicKey> nextKeys, Digest in) {
        return digest(signingThreshold, nextKeys, in.getAlgorithm()).equals(in);
    }

    public static byte[] signingThresholdRepresentation(SigningThreshold threshold) {
        if (threshold instanceof SigningThreshold.Unweighted) {
            return Hex.hexNoPad(((SigningThreshold.Unweighted) threshold).getThreshold()).getBytes(UTF_8);
        } else if (threshold instanceof SigningThreshold.Weighted) {
            return Stream.of(((SigningThreshold.Weighted) threshold).getWeights())
                         .map(lw -> Stream.of(lw).map(KeyConfigurationDigester::weight).collect(joining(",")))
                         .collect(joining(("&")))
                         .getBytes(UTF_8);
        } else {
            throw new IllegalArgumentException("Unknown threshold type: " + threshold.getClass());
        }
    }

    public static String weight(Weight w) {
        if (w.denominator().isEmpty()) {
            return "" + w.numerator();
        }

        return w.numerator() + "/" + w.denominator().get();
    }

}
