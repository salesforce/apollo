package com.salesforce.apollo.stereotomy.processing;

import static java.util.Objects.requireNonNull;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.DelegatedEstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

public interface KeyStateProcessor extends BiFunction<KeyState, KeyEvent, KeyState> {
    
    final static KeyStateProcessor PROCESSOR = new KeyStateProcessor() {
    };

    default KeyState apply(KeyState currentState, KeyEvent event) {
        if (event instanceof InceptionEvent) {
            if (currentState != null) {
                throw new IllegalArgumentException("currentState must not be passed for inception events");
            }
            currentState = initialState((InceptionEvent) event);
        }

        requireNonNull(currentState, "currentState is required");

        var signingThreshold = currentState.getSigningThreshold();
        var keys = currentState.getKeys();
        var nextKeyConfigugurationDigest = currentState.getNextKeyConfigurationDigest();
        var witnessThreshold = currentState.getWitnessThreshold();
        var witnesses = currentState.getWitnesses();
        var lastEstablishmentEvent = currentState.getLastEstablishmentEvent();

        if (event instanceof RotationEvent) {
            var re = (RotationEvent) event;
            signingThreshold = re.getSigningThreshold();
            keys = re.getKeys();
            nextKeyConfigugurationDigest = re.getNextKeyConfiguration();
            witnessThreshold = re.getWitnessThreshold();

            witnesses = new ArrayList<>(witnesses);
            witnesses.removeAll(re.getRemovedWitnesses());
            witnesses.addAll(re.getAddedWitnesses());

            lastEstablishmentEvent = re;
        }
        return newKeyState(currentState.getIdentifier(), signingThreshold, keys,
                           nextKeyConfigugurationDigest.orElse(null), witnessThreshold, witnesses,
                           currentState.configurationTraits(), event, lastEstablishmentEvent,
                           currentState.getDelegatingIdentifier().orElse(null));
    }

    default KeyState initialState(InceptionEvent event) {
        var delegatingPrefix = event instanceof DelegatedInceptionEvent
                ? ((DelegatedEstablishmentEvent) event).getDelegatingEvent().getIdentifier()
                : null;

        return newKeyState(event.getIdentifier(), event.getSigningThreshold(), event.getKeys(),
                           event.getNextKeyConfiguration().orElse(null), event.getWitnessThreshold(),
                           event.getWitnesses(), event.getConfigurationTraits(), event, event, delegatingPrefix);
    }

    default KeyState newKeyState(Identifier identifier,
                                 com.salesforce.apollo.stereotomy.event.SigningThreshold signingThreshold,
                                 List<PublicKey> keys, Digest orElse, int witnessThreshold,
                                 List<BasicIdentifier> witnesses, Set<ConfigurationTrait> configurationTraits,
                                 KeyEvent event, EstablishmentEvent lastEstablishmentEvent, Identifier orElse2) {
        // TODO Auto-generated method stub
        return null;
    }

}
