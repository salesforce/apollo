package com.salesforce.apollo.stereotomy.processing;

import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;

import java.util.ArrayList;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class KeyStateProcessor implements BiFunction<KeyState, KeyEvent, KeyState> {

    private final KEL events;

    public KeyStateProcessor(KEL events) {
        this.events = events;
    }

    @Override
    public KeyState apply(KeyState currentState, KeyEvent event) {
        EstablishmentEvent lastEstablishmentEvent;
        if (event instanceof InceptionEvent) {
            if (currentState != null) {
                throw new IllegalArgumentException("currentState must not be passed for inception events");
            }
            currentState = KeyStateImpl.initialState((InceptionEvent) event, events.getDigestAlgorithm());
            lastEstablishmentEvent = (EstablishmentEvent) event;
        } else if (event instanceof EstablishmentEvent) {
            lastEstablishmentEvent = (EstablishmentEvent) event;
        } else {
            lastEstablishmentEvent = (EstablishmentEvent) events.getKeyEvent(currentState.getLastEstablishmentEvent());
        }

        requireNonNull(currentState, "currentState is required");

        var signingThreshold = currentState.getSigningThreshold();
        var keys = currentState.getKeys();
        var nextKeyConfigugurationDigest = currentState.getNextKeyConfigurationDigest();
        var witnessThreshold = currentState.getWitnessThreshold();
        var witnesses = currentState.getWitnesses();

        if (event instanceof RotationEvent) {
            var re = (RotationEvent) event;
            signingThreshold = re.getSigningThreshold();
            keys = re.getKeys();
            nextKeyConfigugurationDigest = re.getNextKeysDigest();
            witnessThreshold = re.getWitnessThreshold();

            witnesses = new ArrayList<>(witnesses);
            witnesses.removeAll(re.getWitnessesRemovedList());
            witnesses.addAll(re.getWitnessesAddedList());
        }
        KeyState state = KeyStateImpl.newKeyState(event.getIdentifier(), signingThreshold, keys,
                nextKeyConfigugurationDigest.orElse(null), witnessThreshold,
                witnesses, currentState.configurationTraits(), event,
                lastEstablishmentEvent,
                currentState.getDelegatingIdentifier().orElse(null),
                events.getDigestAlgorithm().digest(event.getBytes()));
        return state;
    }

}
