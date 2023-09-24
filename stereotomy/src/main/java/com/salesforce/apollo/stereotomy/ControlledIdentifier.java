/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertExtension;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A controlled identifier, representing the current state of the identifier at
 * all times.
 *
 * @author hal.hildebrand
 */
public interface ControlledIdentifier<D extends Identifier> extends BoundIdentifier<D> {
    /**
     * @return the binding of the identifier to the current key state
     */
    BoundIdentifier<D> bind();

    /**
     * Commit the delegated rotation and commitment to the receiver's KERL
     *
     * @param delegation - the delegated rotation event
     * @param commitment - the event attachment that commits the delegation
     * @return the future commitment of the keystate
     */
    Void commit(DelegatedRotationEvent delegation, AttachmentEvent commitment);

    /**
     * Construct a delegated rotation event
     *
     * @return the future DelegatedRotation event
     */
    DelegatedRotationEvent delegateRotate(RotationSpecification.Builder spec);

    /**
     * @return the KERL of the receiver identifier
     */
    List<EventWithAttachments> getKerl();

    /**
     * @return the Signer for the key state binding
     */
    Signer getSigner();

    /**
     * @return a new ephemeral BasicIdentifier
     */
    Optional<KeyPair> newEphemeral();

    /**
     * Create a new delegated identifier using the receiver as the base.
     */
    <I extends Identifier> ControlledIdentifier<I> newIdentifier(Builder<I> newBuilder);

    /**
     * Provision a certificate that encodes this identifier using a generated Basic
     * Identifier. The certificate returned is signed by this self same generated
     * basic identifier
     * <p>
     * A new key pair is generated and this becomes the signing key of the
     * certificate. This new public key is then signed by this identifier's current
     * key state's key(s)..
     * <p>
     * The values are encoded into the SubjectDN of the certificate as follows:
     * <ul>
     * <li>UID -B64 encoding of the member Identifier</li>
     * <li>DC - The signature of the key state of the identifier in UID of the
     * generated public key that signs the certificate</li>
     * </ul>
     *
     * @param validFrom          - the Instant which the generated certificate
     *                           becomes valid
     * @param valid              - how long the certificate will be valid
     * @param extensions         - any extra stuff to put into ye pot
     * @param signatureAlgorithm - the sig algorithm to use
     * @return a CertificateWithPrivateKey that is self signed by the public key of
     * the X509Certificate
     */
    CertificateWithPrivateKey provision(Instant validFrom, Duration valid,
                                        List<CertExtension> extensions, SignatureAlgorithm algo);

    /**
     * Provision a certificate that encodes this identifier using a generated Basic
     * Identifier. The certificate returned is signed by this self same generated
     * basic identifier
     * <p>
     * A new key pair is generated and this becomes the signing key of the
     * certificate. This new public key is then signed by this identifier's current
     * key state's key(s).
     * <p>
     * The values are encoded into the SubjectDN of the certificate as follows:
     * <ul>
     * <li>UID -B64 encoding of the member Identifier</li>
     * <li>DC - The signature of the key state of the identifier in UID of the
     * generated public key that signs the certificate</li>
     * </ul>
     *
     * @param validFrom          - the Instant which the generated certificate
     *                           becomes valid
     * @param valid              - how long the certificate will be valid
     * @param signatureAlgorithm - the sig algorithm to use
     * @return a CertificateWithPrivateKey that is self signed by the public key of
     * the X509Certificate
     */
    default CertificateWithPrivateKey provision(Instant validFrom, Duration valid,
                                                SignatureAlgorithm algo) {
        return provision(validFrom, valid, Collections.emptyList(), algo);
    }

    /**
     * Rotate the current key state
     */
    Void rotate();

    /**
     * Rotate the current key state using the supplied specification
     */
    Void rotate(RotationSpecification.Builder spec);

    /**
     * Publish the SealingEvent using the supplied specification
     */
    EventCoordinates seal(InteractionSpecification.Builder spec);
}
