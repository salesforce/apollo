/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import static java.util.Collections.disjoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.KeyEventLog;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.DelegatedEstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.InteractionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfSigningIdentifier;
import com.salesforce.apollo.stereotomy.specification.KeyConfigurationDigester;

/**
 * @author hal.hildebrand
 *
 */
public class Validator {

    private static <T> boolean distinct(Collection<T> items) {
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

    private final KeyEventLog kel;

    public Validator(KeyEventLog kel) {
        this.kel = kel;
    }

    public void validateKeyEventData(KeyState state, KeyEvent event) {
        if (event instanceof EstablishmentEvent) {
            var ee = (EstablishmentEvent) event;

            this.validateKeyConfiguration(ee);

            this.validate(ee.getIdentifier().isTransferable() || ee.getNextKeysDigest().isEmpty(),
                          "non-transferable prefix must not have a next key configuration");

            if (event instanceof InceptionEvent) {
                var icp = (InceptionEvent) ee;

                this.validate(icp.getSequenceNumber() == 0, "inception events must have a sequence number of 0");

                this.validateIdentifier(icp);

                this.validateInceptionWitnesses(icp);
            } else if (event instanceof RotationEvent) {
                var rot = (RotationEvent) ee;

                this.validate(!(state.isDelegated()) || rot instanceof DelegatedRotationEvent,
                              "delegated identifiers must use delegated rotation event type");

                this.validate(rot.getSequenceNumber() > 0,
                              "non-inception event must have a sequence number greater than 0 (s: %s)",
                              rot.getSequenceNumber());

                this.validate(event.getIdentifier().isTransferable(),
                              "only transferable identifiers can have rotation events");

                Optional<KeyEvent> lookup = kel.getKeyEvent(state.getLastEstablishmentEvent());
                if (lookup.isEmpty()) {
                    throw new InvalidKeyEventException(String.format("previous establishment event does not exist"));
                }
                EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) lookup.get();
                this.validate(lastEstablishmentEvent.getNextKeysDigest().isPresent(),
                              "previous establishment event must have a next key configuration for rotation");

                var nextKeyConfigurationDigest = lastEstablishmentEvent.getNextKeysDigest().get();
                this.validate(KeyConfigurationDigester.matches(rot.getSigningThreshold(), rot.getKeys(),
                                                               nextKeyConfigurationDigest),
                              "digest of signing threshold and keys must match digest in previous establishment event");

                this.validateRotationWitnesses(rot, state);
            }

            if (event instanceof DelegatedEstablishmentEvent) {
                var dee = (DelegatedEstablishmentEvent) ee;
                var delegatingEvent = this.kel.getKeyEvent(dee.getDelegatingEvent())
                                                        .orElseThrow(() -> new MissingDelegatingEventException(event,
                                                                dee.getDelegatingEvent()));

                this.validate(this.containsSeal(delegatingEvent.getSeals(), dee),
                              "delegated establishment event seal must contain be contained in referenced delegating event");
            }
        } else if (event instanceof InteractionEvent) {
            var ixn = (InteractionEvent) event;

            this.validate(ixn.getSequenceNumber() > 0,
                          "non-inception event must have a sequence number greater than 0 (s: %s)",
                          ixn.getSequenceNumber());

            this.validate(!state.configurationTraits().contains(ConfigurationTrait.ESTABLISHMENT_EVENTS_ONLY),
                          "interaction events only permitted when identifier is not configured for establishment events only");
        }
    }

    private boolean containsSeal(List<Seal> seals, DelegatedEstablishmentEvent event) {
        for (var s : seals) {
            if (s instanceof Seal.CoordinatesSeal) {
                var ecds = (Seal.CoordinatesSeal) s;
                var digest = ecds.getEvent().getDigest();
                if (ecds.getEvent().getIdentifier().equals(event.getIdentifier())
                        && ecds.getEvent().getSequenceNumber() == event.getSequenceNumber()
                        && event.hash(digest.getAlgorithm()).equals(digest)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void validate(boolean valid, String message, Object... formatValues) {
        if (!valid) {
            throw new InvalidKeyEventException(String.format(message, formatValues));
        }
    }

    private void validateIdentifier(InceptionEvent event) {
        if (event.getIdentifier() instanceof BasicIdentifier) {

            this.validate(event.getKeys().size() == 1, "basic identifiers can only have a single key");

            this.validate(((BasicIdentifier) event.getIdentifier()).getPublicKey().equals(event.getKeys().get(0)),
                          "basic identifier key must match event key");

        } else if (event.getIdentifier() instanceof SelfAddressingIdentifier) {
            var sap = (SelfAddressingIdentifier) event.getIdentifier();
            var digest = sap.getDigest().getAlgorithm().digest(event.getInceptionStatement());

            this.validate(sap.getDigest().equals(digest),
                          "self-addressing identifier digests must match digest of inception statement");

        } else if (event.getIdentifier() instanceof SelfSigningIdentifier) {
            var ssp = (SelfSigningIdentifier) event.getIdentifier();

            this.validate(event.getKeys().size() == 1, "self-signing identifiers can only have a single key");

            var ops = SignatureAlgorithm.lookup(event.getKeys().get(0));
            this.validate(ops.verify(event.getKeys().get(0), ssp.getSignature(), event.getInceptionStatement()),
                          "self-signing prefix signature must verify against inception statement");

        } else {
            throw new IllegalArgumentException("Unknown prefix type: " + event.getIdentifier().getClass());
        }
    }

    private void validateInceptionWitnesses(InceptionEvent icp) {
        if (icp.getWitnesses().isEmpty()) {
            this.validate(icp.getWitnessThreshold() == 0, "witness threshold must be 0 if no witnesses are provided");
        } else {
            this.validate(distinct(icp.getWitnesses()), "witness set must not have duplicates");

            this.validate(icp.getWitnessThreshold() > 0,
                          "witness threshold must be greater than 0 if witnesses are provided (given: threshold: %s, witnesses: %s",
                          icp.getWitnessThreshold(), icp.getWitnesses().size());

            this.validate(icp.getWitnessThreshold() <= icp.getWitnesses().size(),
                          "witness threshold must be less than or equal to the number of witnesses (given: threshold: %s, witnesses: %s",
                          icp.getWitnessThreshold(), icp.getWitnesses().size());
        }
    }

    private void validateKeyConfiguration(EstablishmentEvent ee) {
        this.validate(!ee.getKeys().isEmpty(), "establishment events must have at least one key");

        if (ee.getSigningThreshold() instanceof SigningThreshold.Unweighted) {
            this.validate(ee.getKeys()
                            .size() >= ((SigningThreshold.Unweighted) ee.getSigningThreshold()).getThreshold(),
                          "unweighted signing threshold must be less than or equals to the number of keys");
        } else if (ee.getSigningThreshold() instanceof SigningThreshold.Weighted) {
            var weightedThreshold = ((SigningThreshold.Weighted) ee.getSigningThreshold());
            var countOfWeights = SigningThreshold.countWeights(weightedThreshold.getWeights());
            this.validate(ee.getKeys().size() == countOfWeights,
                          "weighted signing threshold must specify a weight for each key");
        }
    }

    private void validateRotationWitnesses(RotationEvent rot, KeyState state) {
        this.validate(distinct(rot.getWitnessesRemovedList()), "removed witnesses must not have duplicates");

        this.validate(distinct(rot.getWitnessesRemovedList()), "added witnesses must not have duplicates");

        this.validate(state.getWitnesses().containsAll(rot.getWitnessesRemovedList()),
                      "removed witnesses must be present witness list");

        this.validate(disjoint(rot.getWitnessesAddedList(), rot.getWitnessesRemovedList()),
                      "added and removed witnesses must be mutually exclusive");

        this.validate(disjoint(rot.getWitnessesAddedList(), state.getWitnesses()),
                      "added witnesses must not already be present in witness list");

        var newWitnesses = new ArrayList<>(state.getWitnesses());
        newWitnesses.removeAll(rot.getWitnessesRemovedList());
        newWitnesses.addAll(rot.getWitnessesAddedList());

        this.validate(rot.getWitnessThreshold() >= 0, "witness threshold must not be negative");

        if (newWitnesses.isEmpty()) {
            this.validate(rot.getWitnessThreshold() == 0, "witness threshold must be 0 if no witnesses are specified");
        } else {
            this.validate(rot.getWitnessThreshold() <= newWitnesses.size(),
                          "witness threshold must be less than or equal to the number of witnesses "
                                  + "(threshold: %s, witnesses: %s)",
                          rot.getWitnessThreshold(), newWitnesses.size());
        }
    }

}
