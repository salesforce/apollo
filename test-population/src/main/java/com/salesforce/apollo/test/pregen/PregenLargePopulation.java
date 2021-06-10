/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.test.pregen;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.cert.Certificates;
import com.salesforce.apollo.utils.Utils;

/**
 * A utility to pre generate CA and member Cert/Key pairs for testing.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class PregenLargePopulation {
    public static final int cardinality = 1000;

    public static CertificateWithPrivateKey getMember(int index) {
        byte[] hash = new byte[32];
        hash[0] = (byte) index;
        KeyPair keyPair = SignatureAlgorithm.ED_25519.generateKeyPair();
        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plusSeconds(10_000));
        Digest id = new Digest(DigestAlgorithm.DEFAULT, hash);
        X509Certificate generated = Certificates.selfSign(false,
                                                          Utils.encode(id, "foo.com", index, keyPair.getPublic()),
                                                          Utils.secureEntropy(), keyPair, notBefore, notAfter,
                                                          Collections.emptyList());
        return new CertificateWithPrivateKey(generated, keyPair.getPrivate());
    }
}
