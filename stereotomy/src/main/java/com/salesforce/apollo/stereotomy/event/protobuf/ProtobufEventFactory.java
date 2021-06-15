/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.salesfoce.apollo.stereotomy.event.proto.Establishment;
import com.salesfoce.apollo.stereotomy.event.proto.EventCommon;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates;
import com.salesfoce.apollo.stereotomy.event.proto.Header;
import com.salesfoce.apollo.stereotomy.event.proto.IdentifierSpec;
import com.salesfoce.apollo.stereotomy.event.proto.InteractionSpec;
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
    private static final String INTERACTION_TYPE               = "ixn";
    @SuppressWarnings("unused")
    private static final String RECEIPT_FROM_BASIC_TYPE        = "rct";
    @SuppressWarnings("unused")
    private static final String RECEIPT_FROM_TRANSFERABLE_TYPE = "vrc";
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
                                                                   .setDigest(((Seal.DigestSeal) s).getDigest()
                                                                                                   .toByteString())
                                                                   .build();
        } else {
            throw new IllegalArgumentException("Unknown seal type: " + s.getClass().getSimpleName());
        }
    }

    public static EventCoordinates.Builder toCoordinates(com.salesforce.apollo.stereotomy.event.EventCoordinates coordinates) {
        return EventCoordinates.newBuilder()
                               .setDigest(coordinates.getDigest().toByteString())
                               .setIdentifier(coordinates.getIdentifier().toByteString())
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
        var bs = identifierSpec(prefix, specification).toByteString();
        var signature = specification.getSigner().sign(bs);

        var common = EventCommon.newBuilder().putAllAuthentication(Map.of(0, signature.toByteString()));

        var builder = com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent.newBuilder();

        return new InceptionEventImpl(builder.setIdentifier(prefix.toByteString())
                                             .setCommon(common)
                                             .setSpecification(inceptionStatement)
                                             .build());
    }

    @Override
    public KeyEvent interaction(InteractionSpecification specification) {
        InteractionSpec ispec = interactionSpec(specification);
        Map<Integer, JohnHancock> signatures = Map.of();

        if (specification.getSigner() != null) {
            var signature = specification.getSigner().sign(ispec.toByteString());
            signatures = Map.of(0, signature);
        }

        var common = EventCommon.newBuilder()
                                .setPrevious(toCoordinates(specification.getPrevious()))
                                .setFormat(specification.getFormat().name())
                                .putAllAuthentication(signatures.entrySet()
                                                                .stream()
                                                                .collect(Collectors.toMap(e -> e.getKey(),
                                                                                          e -> e.getValue()
                                                                                                .toByteString())));
        com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent.Builder builder = com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent.newBuilder();
        return new InteractionEventImpl(builder.setSpecification(ispec).setCommon(common).build());
    }

    @Override
    public RotationEvent rotation(RotationSpecification specification) {
        var rotationSpec = rotationSpec(specification.getIdentifier(), specification);
        Map<Integer, JohnHancock> signatures = Map.of();

        if (specification.getSigner() != null) {
            var signature = specification.getSigner().sign(rotationSpec.toByteArray());
            signatures = Map.of(0, signature);
        }

        var common = EventCommon.newBuilder()
                                .setPrevious(toCoordinates(specification.getPrevious()))
                                .setFormat(specification.getFormat().name())
                                .putAllAuthentication(signatures.entrySet()
                                                                .stream()
                                                                .collect(Collectors.toMap(e -> e.getKey(),
                                                                                          e -> e.getValue()
                                                                                                .toByteString())));
        com.salesfoce.apollo.stereotomy.event.proto.RotationEvent.Builder builder = com.salesfoce.apollo.stereotomy.event.proto.RotationEvent.newBuilder();
        return new RotationEventImpl(builder.setSpecification(rotationSpec).setCommon(common).build());
    }

    com.salesfoce.apollo.stereotomy.event.proto.Version.Builder toVersion(com.salesforce.apollo.stereotomy.event.Version version) {
        return Version.newBuilder().setMajor(version.getMajor()).setMinor(version.getMinor());
    }

    private IdentifierSpec identifierSpec(Identifier identifier, IdentifierSpecification specification) {
        var establishment = Establishment.newBuilder()
                                         .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                                         .addAllKeys(specification.getKeys()
                                                                  .stream()
                                                                  .map(k -> bs(k))
                                                                  .collect(Collectors.toList()))
                                         .setNextKeysDigest((specification.getNextKeys() == null ? Digest.NONE
                                                 : specification.getNextKeys()).toByteString())
                                         .setWitnessThreshold(specification.getWitnessThreshold());
        var header = Header.newBuilder()
                           .setSequenceNumber(0)
                           .setVersion(toVersion(specification.getVersion()))
                           .setPriorEventDigest(Digest.NONE.toByteString())
                           .setIdentifier((identifier == null ? Identifier.NONE : identifier).toByteString())
                           .setEventType(INCEPTION_TYPE);

        return IdentifierSpec.newBuilder()
                             .setHeader(header)
                             .setEstablishment(establishment)
                             .addAllWitnesses(specification.getWitnesses()
                                                           .stream()
                                                           .map(i -> i.toByteString())
                                                           .collect(Collectors.toList()))
                             .addAllConfiguration(specification.getConfigurationTraits()
                                                               .stream()
                                                               .map(ct -> ct.name())
                                                               .collect(Collectors.toList()))
                             .build();
    }

    private InteractionSpec interactionSpec(InteractionSpecification specification) {

        Header header = Header.newBuilder()
                              .setSequenceNumber(0)
                              .setPriorEventDigest((specification.getPriorEventDigest()).toByteString())
                              .setVersion(toVersion(specification.getVersion()))
                              .setIdentifier(specification.getIdentifier().toByteString())
                              .setEventType(INTERACTION_TYPE)
                              .build();

        return InteractionSpec.newBuilder()
                              .setHeader(header)
                              .addAllSeals(specification.getSeals()
                                                        .stream()
                                                        .map(e -> sealOf(e))
                                                        .collect(Collectors.toList()))
                              .build();
    }

    private RotationSpec rotationSpec(Identifier identifier, RotationSpecification specification) {
        var establishment = Establishment.newBuilder()
                                         .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                                         .addAllKeys(specification.getKeys()
                                                                  .stream()
                                                                  .map(k -> bs(k))
                                                                  .collect(Collectors.toList()))
                                         .setNextKeysDigest((specification.getNextKeys() == null ? Digest.NONE
                                                 : specification.getNextKeys()).toByteString())
                                         .setWitnessThreshold(specification.getWitnessThreshold());
        var header = Header.newBuilder()
                           .setSequenceNumber(specification.getSequenceNumber())
                           .setVersion(toVersion(specification.getVersion()))
                           .setPriorEventDigest(specification.getPriorEventDigest().toByteString())
                           .setIdentifier(identifier.toByteString())
                           .setEventType(ROTATION_TYPE);

        return RotationSpec.newBuilder()
                           .setHeader(header)
                           .setEstablishment(establishment)
                           .addAllWitnessesAdded(specification.getAddedWitnesses()
                                                              .stream()
                                                              .map(i -> i.toByteString())
                                                              .collect(Collectors.toList()))
                           .addAllWitnessesRemoved(specification.getRemovedWitnesses()
                                                                .stream()
                                                                .map(i -> i.toByteString())
                                                                .collect(Collectors.toList()))
                           .build();
    }
}
