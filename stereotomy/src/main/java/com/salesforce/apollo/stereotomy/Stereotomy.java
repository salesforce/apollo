/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.cryptography.proto.Sig;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The Controller interface
 *
 * @author hal.hildebrand
 */
public interface Stereotomy {

    static Version currentVersion() {
        return new Version() {

            @Override
            public int getMajor() {
                return 0;
            }

            @Override
            public int getMinor() {
                return 1;
            }
        };
    }

    /**
     * Decode the SubjectDN of the certificate as follows:
     * <ul>
     * <li>CN - Host name of the supplied endpoint</li>
     * <li>L - Port number of supplied endpoint</li>
     * <li>UID - QB64 encoding of the Identifier</li>
     * <li>DC - The signature of the key state of the identifier in the UID of the
     * generated public key that signs the certificate</li>
     * </ul>
     */
    static Optional<Decoded> decode(X509Certificate cert) {
        String dn = cert.getSubjectX500Principal().getName();
        Map<String, String> decoded = decodeDN(dn);
        String id = decoded.get("UID");
        if (id == null) {
            getLogger().warn("Invalid certificate, missing \"UID\" of dn= {}", dn);
            return Optional.empty();
        }

        String signature = decoded.get("DC");
        if (signature == null) {
            getLogger().warn("Invalid certificate, missing \"DC\" of dn= {}", dn);
            return Optional.empty();
        }
        EventCoordinates keyCoords;
        try {
            keyCoords = EventCoordinates.from(EventCoords.parseFrom(Base64.getUrlDecoder().decode(id.getBytes())));
        } catch (InvalidProtocolBufferException e) {
            getLogger().debug("Unable to deserialize key event coordinates", e);
            return Optional.empty();
        }
        Sig sig = null;
        try {
            sig = Sig.parseFrom(Base64.getUrlDecoder().decode(signature.getBytes()));
        } catch (InvalidProtocolBufferException e) {
            getLogger().debug("Unable to deserialize signature", e);
            return Optional.empty();
        }
        return Optional.of(new Decoded(keyCoords, JohnHancock.from(sig)));
    }

    /**
     * Utility to decode the DN into a map of {key,value}s
     */
    static Map<String, String> decodeDN(String dn) {
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

    private static Logger getLogger() {
        return LoggerFactory.getLogger(Stereotomy.class);
    }

    /**
     * Answer the BoundIdentifier of the EventCoordinates
     */
    <D extends Identifier> BoundIdentifier<D> bindingOf(EventCoordinates coordinates);

    /**
     * Publish the delegated inception event, answering the future supplying the ControlledIdentifier for the new key
     * state
     *
     * @param delegation - the inception event for the new identifier
     * @param commitment - the attachment with the seal to the delegation event
     * @return
     */
    ControlledIdentifier<SelfAddressingIdentifier> commit(DelegatedInceptionEvent delegation,
                                                          AttachmentEvent commitment);

    /**
     * Answer the Controllable identifier
     */
    <D extends Identifier> ControlledIdentifier<D> controlOf(D identifier);

    DigestAlgorithm digestAlgorithm();

    /**
     * Answer the KeyState of the provided event coordinates
     */
    KeyState getKeyState(EventCoordinates eventCoordinates);

    /**
     * Answer the KeyState of the key coordinates
     */
    default KeyState getKeyState(KeyCoordinates keyCoordinates) {
        return getKeyState(keyCoordinates.getEstablishmentEvent());
    }

    /**
     * Answer the Verifier for the key coordinates
     */
    Verifier getVerifier(KeyCoordinates coordinates);

    /**
     * Create but do no publish a new delegated identifier.
     *
     * @param controller    - the controlling identifier
     * @param specification - the specification for the delegated identifier
     * @return the created DelegatedInceptionEvent
     */
    DelegatedInceptionEvent newDelegatedIdentifier(Identifier controller,
                                                   Builder<SelfAddressingIdentifier> specification);

    /**
     * Answer a new ControlledIdentifier created from the {@link SelfAddressingIdentifier} prototype and Identifier.NONE
     * as the base identifier
     */
    ControlledIdentifier<SelfAddressingIdentifier> newIdentifier();

    /**
     * Answer a new delegated ControlledIdentifier
     */
    <T extends Identifier> ControlledIdentifier<T> newIdentifier(Identifier controller, Builder<T> specification);

    /**
     * Answer a new ControlledIdentifier created from the supplied specification prototype and Identifier.NONE as the
     * base identifier
     */
    <T extends Identifier> ControlledIdentifier<T> newIdentifier(IdentifierSpecification.Builder<T> spec);

    record Decoded(EventCoordinates coordinates, JohnHancock signature) {
    }
}
