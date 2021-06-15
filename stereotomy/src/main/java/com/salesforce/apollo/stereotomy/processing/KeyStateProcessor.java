package com.salesforce.apollo.stereotomy.processing;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.toCoordinates;
import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.toSigningThreshold;
import static java.util.Objects.requireNonNull;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.salesfoce.apollo.stereotomy.event.proto.StoredKeyState;
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
import com.salesforce.apollo.stereotomy.store.KeyStateImpl;
import com.salesforce.apollo.stereotomy.store.StateStore;

public class KeyStateProcessor implements BiFunction<KeyState, KeyEvent, KeyState> {

    private final StateStore events;

    public KeyStateProcessor(StateStore events) {
        this.events = events;
    }

    public KeyState apply(KeyState currentState, KeyEvent event) {
        EstablishmentEvent lastEstablishmentEvent;
        if (event instanceof InceptionEvent) {
            if (currentState != null) {
                throw new IllegalArgumentException("currentState must not be passed for inception events");
            }
            currentState = initialState((InceptionEvent) event);
            lastEstablishmentEvent = (EstablishmentEvent) event;
        } else if (event instanceof EstablishmentEvent) {
            lastEstablishmentEvent = (EstablishmentEvent) event;
        } else {
            lastEstablishmentEvent = (EstablishmentEvent) events.getKeyEvent(currentState.getLastEstablishmentEvent())
                                                                .get();
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
            nextKeyConfigugurationDigest = re.getNextKeyConfiguration();
            witnessThreshold = re.getWitnessThreshold();

            witnesses = new ArrayList<>(witnesses);
            witnesses.removeAll(re.getRemovedWitnesses());
            witnesses.addAll(re.getAddedWitnesses());
        }
        KeyState state = newKeyState(currentState.getIdentifier(), signingThreshold, keys,
                                     nextKeyConfigugurationDigest.orElse(null), witnessThreshold, witnesses,
                                     currentState.configurationTraits(), event, lastEstablishmentEvent,
                                     currentState.getDelegatingIdentifier().orElse(null));
        events.append(event, state);
        return state;
    }

    private KeyState initialState(InceptionEvent event) {
        var delegatingPrefix = event instanceof DelegatedInceptionEvent
                ? ((DelegatedEstablishmentEvent) event).getDelegatingEvent().getIdentifier()
                : null;

        return newKeyState(event.getIdentifier(), event.getSigningThreshold(), event.getKeys(),
                           event.getNextKeyConfiguration().orElse(null), event.getWitnessThreshold(),
                           event.getWitnesses(), event.getConfigurationTraits(), event, event, delegatingPrefix);
    }

    private KeyState newKeyState(Identifier identifier,
                                 com.salesforce.apollo.stereotomy.event.SigningThreshold signingThreshold,
                                 List<PublicKey> keys, Digest nextKeyConfiguration, int witnessThreshold,
                                 List<BasicIdentifier> witnesses, Set<ConfigurationTrait> configurationTraits,
                                 KeyEvent event, EstablishmentEvent lastEstablishmentEvent,
                                 Identifier delegatingPrefix) {
        return new KeyStateImpl(
                StoredKeyState.newBuilder()
                              .setDigest(event.hash(events.getDigestAlgorithm()).toByteString())
                              .addAllConfigurationTraits(configurationTraits.stream()
                                                                            .map(e -> e.name())
                                                                            .collect(Collectors.toList()))
                              .setCoordinates(toCoordinates(event.getCoordinates()))
                              .setDelegatingIdentifier(delegatingPrefix == null ? Digest.NONE.toByteString()
                                      : delegatingPrefix.toByteString())
                              .setIdentifier(identifier.toByteString())
                              .addAllKeys(keys.stream().map(pk -> bs(pk)).collect(Collectors.toList()))
                              .setLastEstablishmentEvent(toCoordinates(lastEstablishmentEvent.getCoordinates()))
                              .setLastEvent(toCoordinates(event.getCoordinates()))
                              .setNextKeyConfigurationDigest(nextKeyConfiguration == null ? Digest.NONE.toByteString()
                                      : nextKeyConfiguration.toByteString())
                              .setSigningThreshold(toSigningThreshold(signingThreshold))
                              .addAllWitnesses(witnesses.stream()
                                                        .map(e -> e.toByteString())
                                                        .collect(Collectors.toList()))
                              .setWitnessThreshold(witnessThreshold)
                              .build());
    }

}
