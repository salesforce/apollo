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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.stereotomy.Stereotomy.Decoded;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyValidator implements CertificateValidator {

    private final SigningThreshold              threshold;
    private final Map<Integer, BasicIdentifier> validators;

    public StereotomyValidator(Map<Integer, BasicIdentifier> validators, SigningThreshold threshold) {
        this.validators = validators;
        this.threshold = threshold;
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
        var verifier = new Verifier.DefaultVerifier(decoder.keyEvent().getKeys());
        if (!verifier.verify(decoder.signature(), qb64Id)) {
            throw new CertificateException(String.format("Cannot verify cert public key signature for %s", basicId));
        }

        if (!verify(decoder)) {
            throw new CertificateException(String.format("Cannot validate cert identifier for %s",
                                                         decoder.keyEvent().getIdentifier()));
        }
    }

    private boolean verify(Decoded decoded) {
        var attachments = decoded.attachments() == null ? Attachment.EMPTY : decoded.attachments();
        var bytes = decoded.keyEvent().getBytes();
        var verifiedSignatures = new ArrayList<Integer>();
        for (var signature : attachments.endorsements().entrySet()) {

            var verifier = new Verifier.DefaultVerifier(Collections.singletonList(validators.get(signature.getKey())
                                                                                            .getPublicKey()));
            if (verifier.verify(signature.getValue(), bytes)) {
                verifiedSignatures.add(signature.getKey());
            }
        }
        return SigningThreshold.thresholdMet(threshold,
                                             verifiedSignatures.stream().mapToInt(i -> i.intValue()).toArray());
    }
}
