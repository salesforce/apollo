/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.crypto.QualifiedBase64.signature;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;

/**
 * The Controller interface
 * 
 * @author hal.hildebrand
 *
 */
public interface Stereotomy {

    record Decoded(Identifier identifier, JohnHancock signature) {}

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
        return Optional.of(new Decoded(identifier(id), signature(signature)));
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
    <D extends Identifier> CompletableFuture<BoundIdentifier<D>> bindingOf(EventCoordinates coordinates);

    /**
     * Publish the delegated inception event, answering the future supplying the
     * ControlledIdentifier for the new key state
     *
     * @param delegation - the inception event for the new identifier
     * @param commitment - the attachment with the seal to the delegation event
     * @return
     */
    CompletableFuture<ControlledIdentifier<SelfAddressingIdentifier>> commit(DelegatedInceptionEvent delegation,
                                                                             AttachmentEvent commitment);

    /**
     * Answer the Controllable identifier
     */
    <D extends Identifier> CompletableFuture<ControlledIdentifier<D>> controlOf(D identifier);

    DigestAlgorithm digestAlgorithm();

    /**
     * Answer the KeyState of the provided event coordinates
     */
    CompletableFuture<KeyState> getKeyState(EventCoordinates eventCoordinates);

    /**
     * Answer the KeyState of the key coordinates
     */
    default CompletableFuture<KeyState> getKeyState(KeyCoordinates keyCoordinates) {
        return getKeyState(keyCoordinates.getEstablishmentEvent());
    }

    /**
     * Answer the Verifier for the key coordinates
     */
    CompletableFuture<Verifier> getVerifier(KeyCoordinates coordinates);

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
     * Answer a new ControlledIdentifier created from the
     * {@link SelfAddressingIdentifier} prototype and Identifier.NONE as the base
     * identifier
     */
    CompletableFuture<ControlledIdentifier<SelfAddressingIdentifier>> newIdentifier();

    /**
     * Answer a new delegated ControlledIdentifier
     */
    <T extends Identifier> CompletableFuture<ControlledIdentifier<T>> newIdentifier(Identifier controller,
                                                                                    Builder<T> specification);

    /**
     * Answer a new ControlledIdentifier created from the supplied specification
     * prototype and Identifier.NONE as the base identifier
     */
    <T extends Identifier> CompletableFuture<ControlledIdentifier<T>> newIdentifier(IdentifierSpecification.Builder<T> spec);
}
