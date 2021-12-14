/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertExtension;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

/**
 * A controlled identifier, representing the current state of the identifier at
 * all times.
 * 
 * @author hal.hildebrand
 *
 */
public interface ControllableIdentifier extends BoundIdentifier {
    /**
     * @return the binding of the identifier to the current key state
     */
    BoundIdentifier bind();

    /**
     * @param keyIndex
     * @return the Signer for the key state binding
     */
    Optional<Signer> getSigner(int keyIndex);

    /**
     * Provision a certificate that encodes the host, port and this identifier using
     * a generated Basic Identifier. The certificate returned is signed by this self
     * same generated basic identifier
     * 
     * @param endpoint           - the InetSocketAddress of the server side endpoint
     * @param validFrom          - the Instant which the generated certificate
     *                           becomes valid
     * @param valid              - how long the certificate will be valid
     * @param extensions         - any extra stuff to put into ye pot
     * @param signatureAlgorithm - the sig algorithm to use
     * @return a CertificateWithPrivateKey that is self signed by the public key of
     *         the X509Certificate
     */
    Optional<CertificateWithPrivateKey> provision(InetSocketAddress endpoint, Instant validFrom, Duration valid,
                                                  List<CertExtension> extensions, SignatureAlgorithm algo);

    /**
     * Provision a certificate that encodes the host, port and this identifier using
     * a generated Basic Identifier. The certificate returned is signed by this self
     * same generated basic identifier
     * 
     * @param endpoint           - the InetSocketAddress of the server side endpoint
     * @param validFrom          - the Instant which the generated certificate
     *                           becomes valid
     * @param valid              - how long the certificate will be valid
     * @param signatureAlgorithm - the sig algorithm to use
     * @return a CertificateWithPrivateKey that is self signed by the public key of
     *         the X509Certificate
     */
    default Optional<CertificateWithPrivateKey> provision(InetSocketAddress endpoint, Instant validFrom, Duration valid,
                                                          SignatureAlgorithm algo) {
        return provision(endpoint, validFrom, valid, Collections.emptyList(), algo);
    }

    /**
     * Rotate the current key state
     */
    void rotate();

    /**
     * Rotate the current key state using the supplied seals
     */
    void rotate(List<Seal> seals);

    /**
     * Rotate the current key state using the supplied specification
     */
    void rotate(RotationSpecification.Builder spec);

    /**
     * Publish the SealingEvent using the supplied specification
     */
    void seal(InteractionSpecification.Builder spec);

    /**
     * Publish the SealingEvent using the supplied seals
     */
    void seal(List<Seal> seals);

    /**
     * @return the EventSignature for the supplied event
     */
    Optional<EventSignature> sign(KeyEvent event);

}
