/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;

/**
 * The Controller interface
 * 
 * @author hal.hildebrand
 *
 */
public interface Stereotomy {

    record Decoded(EstablishmentEvent keyEvent, Attachment attachments, InetSocketAddress endpoint,
                   JohnHancock signature) {
        public Identifier identifier() {
            return keyEvent.getIdentifier();
        }

        public Verifier verifier() {
            return new Verifier.DefaultVerifier(keyEvent.getKeys());
        }
    }

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
     * <li>UID - QB64 encoding of the KeyCoordinates of the keystate used</li>
     * <li>DC - The signature of the key state of the coordinates in UID of the
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
        String portString = decoded.get("L");
        if (portString == null) {
            getLogger().warn("Invalid certificate, missing \"L\" of dn= {}", dn);
            return Optional.empty();
        }
        int port = Integer.parseInt(portString);

        String hostName = decoded.get("CN");
        if (hostName == null) {
            getLogger().warn("Invalid certificate, missing \\\"CN\\\" of dn= {}", dn);
            return Optional.empty();
        }

        String signature = decoded.get("DC");
        if (signature == null) {
            getLogger().warn("Invalid certificate, missing \\\"DC\\\" of dn= {}", dn);
            return Optional.empty();
        }
        var ewa = EventWithAttachments.fromBase64(id);
        if (ewa.event() instanceof EstablishmentEvent ee) {
            return Optional.of(new Decoded(ee, ewa.attachments(), new InetSocketAddress(hostName, port),
                                           signature(signature)));
        } else {
            getLogger().warn("Invalid certificate, keyEvent is not an EstablishmentEvent: {}",
                             ewa.event().getClass().getSimpleName());
            return Optional.empty();
        }
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
    <D extends Identifier> Optional<BoundIdentifier<D>> bindingOf(EventCoordinates coordinates);

    /**
     * Answer the Controllable identifier
     */
    <D extends Identifier> Optional<ControlledIdentifier<D>> controlOf(D identifier);

    /**
     * Answer the KeyState of the provided event coordinates
     */
    Optional<KeyState> getKeyState(EventCoordinates eventCoordinates);

    /**
     * Answer the KeyState of the key coordinates
     */
    default Optional<KeyState> getKeyState(KeyCoordinates keyCoordinates) {
        return getKeyState(keyCoordinates.getEstablishmentEvent());
    }

    /**
     * Answer the Verifier for the key coordinates
     */
    Optional<Verifier> getVerifier(KeyCoordinates coordinates);

    /**
     * Answer a new ControlledIdentifier created from the default specification
     * prototype and Identifier.NONE as the base identifier
     */
    Optional<ControlledIdentifier<? extends Identifier>> newIdentifier();

    /**
     * Answer a new ControlledIdentifier created from the supplied specification
     * prototype and Identifier.NONE as the base identifier
     */
    Optional<ControlledIdentifier<? extends Identifier>> newIdentifier(IdentifierSpecification.Builder<? super Identifier> spec);
}
