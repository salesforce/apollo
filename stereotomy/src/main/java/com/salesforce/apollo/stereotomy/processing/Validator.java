/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.*;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfSigningIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.KeyConfigurationDigester;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.disjoint;

/**
 * @author hal.hildebrand
 */
public interface Validator {
    static final Logger log = LoggerFactory.getLogger(Validator.class);

    static <T> boolean distinct(Collection<T> items) {
        if (items instanceof Set) {
            return true;
        }

        var set = new HashSet<T>();
        for (var i : items) {
            if (!set.add(i)) {
                return false;
            }
        }

        return true;
    }

    default boolean validate(Identifier identifier, JohnHancock signature, InputStream message, KEL kel) {
        KeyState currentState = kel.getKeyState(identifier);
        if (currentState == null) {
            log.debug("Identifier: {} not found in KeyState", identifier);
            return false;
        }
        for (KeyEvent lee = kel.getKeyEvent(currentState.getLastEstablishmentEvent()); lee != null; lee = kel.getKeyEvent(lee.getPrevious())) {
            var lastEstablishment = (EstablishmentEvent) lee;
            lastEstablishment.getKeys();

            if (new DefaultVerifier(lastEstablishment.getKeys()).verify(lastEstablishment.getSigningThreshold(),
                    signature, message)) {
                return true;
            }
        }
        log.debug("Unable to traverse establistment event chain for: {}", identifier);
        return false;
    }

    default void validateKeyEventData(KeyState state, KeyEvent event, KEL kel) {
        if (event instanceof EstablishmentEvent ee) {
            validateKeyConfiguration(ee);

            validate(ee.getIdentifier().isTransferable() || ee.getNextKeysDigest().isEmpty(),
                    "non-transferable prefix must not have a next key configuration");

            if (event instanceof InceptionEvent icp) {
                validate(icp.getSequenceNumber().equals(ULong.valueOf(0)),
                        "inception events must have a sequence number of 0");
                validateIdentifier(icp);
                validateInceptionWitnesses(icp);
            } else if (event instanceof RotationEvent rot) {
                validate(!(state.isDelegated()) || rot instanceof DelegatedRotationEvent,
                        "delegated identifiers must use delegated rotation event type");

                validate(rot.getSequenceNumber().compareTo(ULong.valueOf(0)) > 0,
                        "non-inception event must have a sequence number greater than 0 (s: %s)",
                        rot.getSequenceNumber());

                validate(event.getIdentifier().isTransferable(),
                        "only transferable identifiers can have rotation events");

                KeyEvent lookup;
                lookup = kel.getKeyEvent(state.getLastEstablishmentEvent());
                if (lookup == null) {
                    throw new InvalidKeyEventException(String.format("previous establishment event does not exist"));
                }
                EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) lookup;
                validate(lastEstablishmentEvent.getNextKeysDigest().isPresent(),
                        "previous establishment event must have a next key configuration for rotation");

                var nextKeyConfigurationDigest = lastEstablishmentEvent.getNextKeysDigest().get();
                validate(KeyConfigurationDigester.matches(rot.getSigningThreshold(), rot.getKeys(),
                                nextKeyConfigurationDigest),
                        "digest of signing threshold and keys must match digest in previous establishment event");

                validateRotationWitnesses(rot, state);
            }

            if (event instanceof DelegatedInceptionEvent dee) {
                validate(dee.getDelegatingPrefix() != null,
                        "delegated establishment event must contain referenced delegating identifier");
            }
        } else if (event instanceof InteractionEvent ixn) {
            validate(ixn.getSequenceNumber().compareTo(ULong.valueOf(0)) > 0,
                    "non-inception event must have a sequence number greater than 0 (s: %s)", ixn.getSequenceNumber());

            validate(!state.configurationTraits().contains(ConfigurationTrait.ESTABLISHMENT_EVENTS_ONLY),
                    "interaction events only permitted when identifier is not configured for establishment events only");
        }
    }

    private void validate(boolean valid, String message, Object... formatValues) {
        if (!valid) {
            throw new InvalidKeyEventException(String.format(message, formatValues));
        }
    }

    private void validateIdentifier(InceptionEvent event) {
        if (event.getIdentifier() instanceof BasicIdentifier bi) {
            validate(event.getKeys().size() == 1, "basic identifiers can only have a single key");

            validate(bi.getPublicKey().equals(event.getKeys().get(0)), "basic identifier key must match event key");

        } else if (event.getIdentifier() instanceof SelfAddressingIdentifier sap) {
            var digest = sap.getDigest().getAlgorithm().digest(event.getInceptionStatement());

            validate(sap.getDigest().equals(digest),
                    "self-addressing identifier digests must match digest of inception statement");

        } else if (event.getIdentifier() instanceof SelfSigningIdentifier ssp) {
            validate(event.getKeys().size() == 1, "self-signing identifiers can only have a single key");

            var ops = SignatureAlgorithm.lookup(event.getKeys().get(0));
            new DefaultVerifier(event.getKeys()).verify(event.getSigningThreshold(), ssp.getSignature(),
                    event.getInceptionStatement());
            validate(ops.verify(event.getKeys().get(0), ssp.getSignature(), event.getInceptionStatement()),
                    "self-signing prefix signature must verify against inception statement");

        } else {
            throw new IllegalArgumentException("Unknown prefix type: " + event.getIdentifier().getClass());
        }
    }

    private void validateInceptionWitnesses(InceptionEvent icp) {
        if (icp.getWitnesses().isEmpty()) {
            validate(icp.getWitnessThreshold() == 0, "witness threshold must be 0 if no witnesses are provided");
        } else {
            validate(distinct(icp.getWitnesses()), "witness set must not have duplicates");

            validate(icp.getWitnessThreshold() > 0,
                    "witness threshold must be greater than 0 if witnesses are provided (given: threshold: %s, witnesses: %s",
                    icp.getWitnessThreshold(), icp.getWitnesses().size());

            validate(icp.getWitnessThreshold() <= icp.getWitnesses().size(),
                    "witness threshold must be less than or equal to the number of witnesses (given: threshold: %s, witnesses: %s",
                    icp.getWitnessThreshold(), icp.getWitnesses().size());
        }
    }

    private void validateKeyConfiguration(EstablishmentEvent ee) {
        validate(!ee.getKeys().isEmpty(), "establishment events must have at least one key");

        if (ee.getSigningThreshold() instanceof SigningThreshold.Unweighted) {
            validate(ee.getKeys().size() >= ((SigningThreshold.Unweighted) ee.getSigningThreshold()).getThreshold(),
                    "unweighted signing threshold must be less than or equals to the number of keys");
        } else if (ee.getSigningThreshold() instanceof SigningThreshold.Weighted) {
            var weightedThreshold = ((SigningThreshold.Weighted) ee.getSigningThreshold());
            var countOfWeights = SigningThreshold.countWeights(weightedThreshold.getWeights());
            validate(ee.getKeys().size() == countOfWeights,
                    "weighted signing threshold must specify a weight for each key");
        }
    }

    private void validateRotationWitnesses(RotationEvent rot, KeyState state) {
        validate(distinct(rot.getWitnessesRemovedList()), "removed witnesses must not have duplicates");

        validate(distinct(rot.getWitnessesRemovedList()), "added witnesses must not have duplicates");

        validate(state.getWitnesses().containsAll(rot.getWitnessesRemovedList()),
                "removed witnesses must be present witness list");

        validate(disjoint(rot.getWitnessesAddedList(), rot.getWitnessesRemovedList()),
                "added and removed witnesses must be mutually exclusive");

        validate(disjoint(rot.getWitnessesAddedList(), state.getWitnesses()),
                "added witnesses must not already be present in witness list");

        var newWitnesses = new ArrayList<>(state.getWitnesses());
        newWitnesses.removeAll(rot.getWitnessesRemovedList());
        newWitnesses.addAll(rot.getWitnessesAddedList());

        validate(rot.getWitnessThreshold() >= 0, "witness threshold must not be negative");

        if (newWitnesses.isEmpty()) {
            validate(rot.getWitnessThreshold() == 0, "witness threshold must be 0 if no witnesses are specified");
        } else {
            validate(rot.getWitnessThreshold() <= newWitnesses.size(),
                    "witness threshold must be less than or equal to the number of witnesses "
                            + "(threshold: %s, witnesses: %s)", rot.getWitnessThreshold(), newWitnesses.size());
        }
    }
}
