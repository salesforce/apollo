/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Optional;

import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.stereotomy.Stereotomy.Decoded;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyValidator implements CertificateValidator {

    private final KEL kel;

    public StereotomyValidator(KEL kel) {
        this.kel = kel;
    }

    @Override
    public void validateClient(X509Certificate[] chain) throws CertificateException {
        validate(chain[0]);
    }

    @Override
    public void validateServer(X509Certificate[] chain) throws CertificateException {
        validate(chain[0]);
    }

    void validate(final X509Certificate cert) throws CertificateException {
        var publicKey = cert.getPublicKey();
        var basicId = new BasicIdentifier(publicKey);

        var decoded = Stereotomy.decode(cert);
        if (decoded.isEmpty()) {
            throw new CertificateException();
        }
        final var qb64Id = qb64(basicId);
        Decoded decoder = decoded.get();
        Optional<Verifier> verifier = decoder.verifier(kel);
        if (verifier.isEmpty()) {
            throw new CertificateException("Cannot get verifier for: " + decoder.coordinates());
        }
        if (!verifier.get().verify(decoder.signature(), qb64Id)) {
            throw new CertificateException(String.format("Cannot verify signature for %s", decoder.coordinates()));
        }
    }

}
