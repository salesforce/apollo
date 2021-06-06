/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.salesfoce.apollo.stereotomy.event.proto.Establishment;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates;
import com.salesfoce.apollo.stereotomy.event.proto.Header;
import com.salesfoce.apollo.stereotomy.event.proto.IdentifierSpec;
import com.salesfoce.apollo.stereotomy.event.proto.RotationSpec;
import com.salesfoce.apollo.stereotomy.event.proto.Version;
import com.salesfoce.apollo.stereotomy.event.proto.Weights;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.Stereotomy.EventFactory;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.SigningThreshold.Weighted.Weight;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.specification.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.specification.InteractionSpecification;
import com.salesforce.apollo.stereotomy.specification.RotationSpecification;

/**
 * @author hal.hildebrand
 *
 */
public class ProtobufEventFactory implements EventFactory {

    @SuppressWarnings("unused")
    private static final String DELEGATED_INCEPTION_TYPE       = "dip";
    @SuppressWarnings("unused")
    private static final String DELEGATED_ROTATION_TYPE        = "drt";
    private static final String INCEPTION_TYPE                 = "icp";
    @SuppressWarnings("unused")
    private static final String INTERACTION_TYPE               = "ixn";
    @SuppressWarnings("unused")
    private static final String RECEIPT_FROM_BASIC_TYPE        = "rct";
    @SuppressWarnings("unused")
    private static final String RECEIPT_FROM_TRANSFERABLE_TYPE = "vrc";
    @SuppressWarnings("unused")
    private static final String ROTATION_TYPE                  = "rot";

    public static Seal sealOf(com.salesfoce.apollo.stereotomy.event.proto.Seal s) {
        if (s.hasCoordinates()) {
            com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates coordinates = s.getCoordinates();
            return new Seal.CoordinatesSeal() {

                @Override
                public com.salesforce.apollo.stereotomy.event.EventCoordinates getEvent() {
                    return new com.salesforce.apollo.stereotomy.event.EventCoordinates(
                            identifier(coordinates.getIdentifier()), coordinates.getSequenceNumber(),
                            digest(coordinates.getDigest()));
                }
            };
        }

        return new Seal.DigestSeal() {

            @Override
            public Digest getDigest() {
                return digest(s.getDigest());
            }

        };
    }

    public static com.salesfoce.apollo.stereotomy.event.proto.Seal sealOf(Seal s) {
        if (s instanceof Seal.CoordinatesSeal) {
            return com.salesfoce.apollo.stereotomy.event.proto.Seal.newBuilder()
                                                                   .setCoordinates(toCoordinates(((Seal.CoordinatesSeal) s).getEvent()))
                                                                   .build();
        } else if (s instanceof Seal.DigestSeal) {
            return com.salesfoce.apollo.stereotomy.event.proto.Seal.newBuilder()
                                                                   .setDigest(qb64(((Seal.DigestSeal) s).getDigest()))
                                                                   .build();
        } else {
            throw new IllegalArgumentException("Unknown seal type: " + s.getClass().getSimpleName());
        }
    }

    public static EventCoordinates.Builder toCoordinates(com.salesforce.apollo.stereotomy.event.EventCoordinates coordinates) {
        return EventCoordinates.newBuilder()
                               .setDigest(qb64(coordinates.getDigest()))
                               .setIdentifier(qb64(coordinates.getIdentifier()))
                               .setSequenceNumber(coordinates.getSequenceNumber());
    }

    public static com.salesforce.apollo.stereotomy.event.EventCoordinates toCoordinates(EventCoordinates coordinates) {
        return new com.salesforce.apollo.stereotomy.event.EventCoordinates(identifier(coordinates.getIdentifier()),
                coordinates.getSequenceNumber(), digest(coordinates.getDigest()));
    }

    public static SigningThreshold toSigningThreshold(com.salesfoce.apollo.stereotomy.event.proto.SigningThreshold signingThreshold) {
        if (signingThreshold.getWeightsCount() == 0) {
            return new SigningThreshold.Unweighted() {
                @Override
                public int getThreshold() {
                    return signingThreshold.getThreshold();
                }

            };
        } else {
            return new SigningThreshold.Weighted() {

                @Override
                public Weight[][] getWeights() {
                    Weight[][] weights = new Weight[signingThreshold.getWeightsCount()][];
                    signingThreshold.getWeightsList()
                                    .stream()
                                    .map(w -> weightArrayFrom(w))
                                    .collect(Collectors.toList())
                                    .toArray(new Weight[signingThreshold.getWeightsCount()][]);
                    return weights;
                }

            };
        }
    }

    public static com.salesfoce.apollo.stereotomy.event.proto.SigningThreshold.Builder toSigningThreshold(SigningThreshold signingThreshold) {
        var builder = com.salesfoce.apollo.stereotomy.event.proto.SigningThreshold.newBuilder();
        if (signingThreshold instanceof SigningThreshold.Unweighted) {
            builder.setThreshold(((SigningThreshold.Unweighted) signingThreshold).getThreshold());
        } else if (signingThreshold instanceof SigningThreshold.Weighted) {
            Weight[][] weights = ((SigningThreshold.Weighted) signingThreshold).getWeights();
            for (Weight[] wa : weights) {
                Weights.Builder wb = Weights.newBuilder();
                for (Weight w : wa) {
                    wb.addWeights(com.salesfoce.apollo.stereotomy.event.proto.Weight.newBuilder()
                                                                                    .setNumerator(w.numerator())
                                                                                    .setDenominator(w.denominator()
                                                                                                     .orElse(0)));
                }
                builder.addWeights(wb);
            }
        }
        return builder;
    }

    private static Weight[] weightArrayFrom(Weights w) {
        Weight[] weights = new Weight[w.getWeightsCount()];
        for (int i = 0; i < weights.length; i++) {
            weights[i] = weightFrom(w.getWeights(i));
        }
        return weights;
    }

    private static Weight weightFrom(com.salesfoce.apollo.stereotomy.event.proto.Weight w) {
        return new Weight() {

            @Override
            public Optional<Integer> denominator() {
                return Optional.ofNullable(w.getDenominator());
            }

            @Override
            public int numerator() {
                return w.getNumerator();
            }
        };

    }

    @Override
    public InceptionEvent inception(IdentifierSpecification specification) {
        var inceptionStatement = identifierSpec(null, specification);
        var prefix = Identifier.identifier(specification, inceptionStatement.toByteArray());
        var bytes = identifierSpec(prefix, specification);
        var signature = specification.getSigner().sign(bytes.toByteArray());
        var establishment = Establishment.newBuilder()
                                         .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                                         .addAllKeys(specification.getKeys()
                                                                  .stream()
                                                                  .map(k -> qb64(k))
                                                                  .collect(Collectors.toList()))
                                         .setNextKeyConfiguration(qb64(specification.getNextKeys() == null ? Digest.NONE
                                                 : specification.getNextKeys()))
                                         .setWitnessThreshold(specification.getWitnessThreshold());

        var header = Header.newBuilder()
                           .setSequenceNumber(0)
                           .setVersion(toVersion(specification.getVersion()))
                           .setFormat(specification.getFormat().name())
                           .setIdentifier(qb64(prefix))
                           .putAllAuthentication(Map.of(0, qb64(signature)));

        var builder = com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent.newBuilder();
        builder.setHeader(header)
               .setEstablishment(establishment)
               .setInceptionStatement(inceptionStatement)
               .addAllWitnesses(specification.getWitnesses().stream().map(i -> qb64(i)).collect(Collectors.toList()))
               .addAllConfigurationTraits(specification.getConfigurationTraits()
                                                       .stream()
                                                       .map(ct -> ct.name())
                                                       .collect(Collectors.toList()));
        return new InceptionEventImpl(builder.build());
    }

    @Override
    public KeyEvent interaction(InteractionSpecification specification) {
        com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent.Builder builder = com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent.newBuilder();
        return new InteractionEventImpl(builder.build());
    }

    @Override
    public RotationEvent rotation(RotationSpecification specification) {
        var rotationSpec = rotationSpec(specification.getIdentifier(), specification);
        Map<Integer, JohnHancock> signatures = Map.of();

        if (specification.getSigner() != null) {
            var signature = specification.getSigner().sign(rotationSpec.toByteArray());
            signatures = Map.of(0, signature);
        }

        var establishment = Establishment.newBuilder()
                                         .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                                         .addAllKeys(specification.getKeys()
                                                                  .stream()
                                                                  .map(k -> qb64(k))
                                                                  .collect(Collectors.toList()))
                                         .setNextKeyConfiguration(qb64(specification.getNextKeys() == null ? Digest.NONE
                                                 : specification.getNextKeys()))
                                         .setWitnessThreshold(specification.getWitnessThreshold());

        var header = Header.newBuilder() 
                           .setPrevious(toCoordinates(specification.getPrevious()))
                           .setPreviousDigest(qb64(specification.getPriorEventDigest()))
                           .setVersion(toVersion(specification.getVersion()))
                           .setFormat(specification.getFormat().name())
                           .setIdentifier(qb64(specification.getIdentifier()))
                           .putAllAuthentication(signatures.entrySet()
                                                           .stream()
                                                           .collect(Collectors.toMap(e -> e.getKey(),
                                                                                     e -> qb64(e.getValue()))));

        com.salesfoce.apollo.stereotomy.event.proto.RotationEvent.Builder builder = com.salesfoce.apollo.stereotomy.event.proto.RotationEvent.newBuilder();
        builder.addAllAddedWitnesses(specification.getAddedWitnesses()
                                                  .stream()
                                                  .map(b -> qb64(b))
                                                  .collect(Collectors.toList()));
        builder.addAllRemovedWitnesses(specification.getRemovedWitnesses()
                                                    .stream()
                                                    .map(b -> qb64(b))
                                                    .collect(Collectors.toList()));
        builder.addAllSeals(specification.getSeals().stream().map(s -> sealOf(s)).collect(Collectors.toList()));
        return new RotationEventImpl(builder.setHeader(header).setEstablishment(establishment).build());
    }

    com.salesfoce.apollo.stereotomy.event.proto.Version.Builder toVersion(com.salesforce.apollo.stereotomy.event.Version version) {
        return Version.newBuilder().setMajor(version.getMajor()).setMinor(version.getMinor());
    }

    private IdentifierSpec identifierSpec(Identifier identifier, IdentifierSpecification specification) {
        return IdentifierSpec.newBuilder()
                             .setVersion(toVersion(specification.getVersion()))
                             .setIdentifier(qb64(identifier == null ? Identifier.NONE : identifier))
                             .setSequenceNumber(0)
                             .setEventType(INCEPTION_TYPE)
                             .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                             .addAllKeys(specification.getKeys()
                                                      .stream()
                                                      .map(k -> qb64(k))
                                                      .collect(Collectors.toList()))
                             .setNextKeysDigest(qb64(specification.getNextKeys() == null ? Digest.NONE
                                     : specification.getNextKeys()))
                             .setWitnessThreshold(specification.getWitnessThreshold())
                             .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                             .addAllWitnesses(specification.getWitnesses()
                                                           .stream()
                                                           .map(i -> qb64(i))
                                                           .collect(Collectors.toList()))
                             .addAllConfiguration(specification.getConfigurationTraits()
                                                               .stream()
                                                               .map(ct -> ct.name())
                                                               .collect(Collectors.toList()))
                             .build();
    }

    private RotationSpec rotationSpec(Identifier identifier, RotationSpecification specification) {
        return RotationSpec.newBuilder()
                           .setVersion(toVersion(specification.getVersion()))
                           .setIdentifier(qb64(identifier == null ? Identifier.NONE : identifier))
                           .setSequenceNumber(specification.getSequenceNumber())
                           .setPriorEventDigest(qb64(specification.getPriorEventDigest()))
                           .setEventType(INCEPTION_TYPE)
                           .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                           .addAllKeys(specification.getKeys().stream().map(k -> qb64(k)).collect(Collectors.toList()))
                           .setNextKeysDigest(qb64(specification.getNextKeys() == null ? Digest.NONE
                                   : specification.getNextKeys()))
                           .setWitnessThreshold(specification.getWitnessThreshold())
                           .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                           .addAllWitnessesAdded(specification.getAddedWitnesses()
                                                              .stream()
                                                              .map(i -> qb64(i))
                                                              .collect(Collectors.toList()))
                           .addAllWitnessesRemoved(specification.getRemovedWitnesses()
                                                                .stream()
                                                                .map(i -> qb64(i))
                                                                .collect(Collectors.toList()))
                           .build();
    }
}
