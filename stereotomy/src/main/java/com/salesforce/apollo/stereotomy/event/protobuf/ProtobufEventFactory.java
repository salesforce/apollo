/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.DELEGATED_INCEPTION_TYPE;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.DELEGATED_ROTATION_TYPE;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.INCEPTION_TYPE;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.INTERACTION_TYPE;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.ROTATION_TYPE;

import java.util.Optional;
import java.util.stream.Collectors;

import com.salesfoce.apollo.stereotomy.event.proto.Establishment;
import com.salesfoce.apollo.stereotomy.event.proto.EventCommon;
import com.salesfoce.apollo.stereotomy.event.proto.Header;
import com.salesfoce.apollo.stereotomy.event.proto.IdentifierSpec;
import com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.InteractionSpec;
import com.salesfoce.apollo.stereotomy.event.proto.RotationSpec;
import com.salesfoce.apollo.stereotomy.event.proto.Version;
import com.salesfoce.apollo.stereotomy.event.proto.Weights;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.SigningThreshold.Weighted.Weight;
import com.salesforce.apollo.stereotomy.event.EventFactory;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

/**
 * @author hal.hildebrand
 *
 */
public class ProtobufEventFactory implements EventFactory {

    public static KeyEvent toKeyEvent(byte[] event, String ilk) {
        try {
            return switch (ilk) {
            case ROTATION_TYPE -> new RotationEventImpl(com.salesfoce.apollo.stereotomy.event.proto.RotationEvent.parseFrom(event));
            case DELEGATED_INCEPTION_TYPE -> new DelegatedInceptionEventImpl(com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent.parseFrom(event));
            case DELEGATED_ROTATION_TYPE -> new DelegatedRotationEventImpl(com.salesfoce.apollo.stereotomy.event.proto.RotationEvent.parseFrom(event));
            case INCEPTION_TYPE -> new InceptionEventImpl(com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent.parseFrom(event));
            case INTERACTION_TYPE -> new InteractionEventImpl(InteractionEvent.parseFrom(event));
            default -> null;
            };
        } catch (Throwable e) {
            return null;
        }
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
                    signingThreshold.getWeightsList().stream().map(w -> weightArrayFrom(w)).collect(Collectors.toList())
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
    public InceptionEvent inception(Identifier identifier, IdentifierSpecification specification) {
        var inceptionStatement = identifierSpec(identifier, specification);

        var prefix = Identifier.identifier(specification, inceptionStatement.toByteString().asReadOnlyByteBuffer());
        var bs = identifierSpec(prefix, specification).toByteString();

        var common = EventCommon.newBuilder().setAuthentication(specification.getSigner().sign(bs).toSig());

        var builder = com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent.newBuilder();

        return new InceptionEventImpl(builder.setIdentifier(prefix.toIdent()).setCommon(common)
                                             .setSpecification(inceptionStatement).build());
    }

    @Override
    public KeyEvent interaction(InteractionSpecification specification) {
        InteractionSpec ispec = interactionSpec(specification);
        final var bs = ispec.toByteString();
        var signatures = specification.getSigner().sign(bs).toSig();
        var common = EventCommon.newBuilder().setPrevious(specification.getPrevious().toEventCoords())
                                .setAuthentication(signatures);
        com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent.Builder builder = com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent.newBuilder();
        return new InteractionEventImpl(builder.setSpecification(ispec).setCommon(common).build());
    }

    @Override
    public RotationEvent rotation(RotationSpecification specification) {
        var rotationSpec = rotationSpec(specification.getIdentifier(), specification);

        final var bs = rotationSpec.toByteString();
        var signatures = specification.getSigner().sign(bs).toSig();

        var common = EventCommon.newBuilder().setPrevious(specification.getPrevious().toEventCoords())
                                .setAuthentication(signatures);
        var builder = com.salesfoce.apollo.stereotomy.event.proto.RotationEvent.newBuilder();
        return new RotationEventImpl(builder.setSpecification(rotationSpec).setCommon(common).build());
    }

    com.salesfoce.apollo.stereotomy.event.proto.Version.Builder toVersion(com.salesforce.apollo.stereotomy.event.Version version) {
        return Version.newBuilder().setMajor(version.getMajor()).setMinor(version.getMinor());
    }

    private IdentifierSpec identifierSpec(Identifier identifier, IdentifierSpecification specification) {
        var establishment = Establishment.newBuilder()
                                         .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                                         .addAllKeys(specification.getKeys().stream().map(k -> bs(k))
                                                                  .collect(Collectors.toList()))
                                         .setNextKeysDigest((specification.getNextKeys() == null ? Digest.NONE
                                                                                                 : specification.getNextKeys()).toDigeste())
                                         .setWitnessThreshold(specification.getWitnessThreshold());
        var header = Header.newBuilder().setSequenceNumber(0).setVersion(toVersion(specification.getVersion()))
                           .setPriorEventDigest(Digest.NONE.toDigeste()).setIdentifier(identifier.toIdent())
                           .setIlk(INCEPTION_TYPE);

        return IdentifierSpec.newBuilder().setHeader(header).setEstablishment(establishment)
                             .addAllWitnesses(specification.getWitnesses().stream().map(i -> i.toIdent())
                                                           .collect(Collectors.toList()))
                             .addAllConfiguration(specification.getConfigurationTraits().stream().map(ct -> ct.name())
                                                               .collect(Collectors.toList()))
                             .build();
    }

    private InteractionSpec interactionSpec(InteractionSpecification specification) {

        Header header = Header.newBuilder().setSequenceNumber(specification.getSequenceNumber())
                              .setPriorEventDigest((specification.getPriorEventDigest()).toDigeste())
                              .setVersion(toVersion(specification.getVersion()).setFormat(specification.getFormat()
                                                                                                       .name()))
                              .setIdentifier(specification.getIdentifier().toIdent()).setIlk(INTERACTION_TYPE).build();

        return InteractionSpec.newBuilder().setHeader(header)
                              .addAllSeals(specification.getSeals().stream().map(e -> e.toSealed())
                                                        .collect(Collectors.toList()))
                              .build();
    }

    private RotationSpec rotationSpec(Identifier identifier, RotationSpecification specification) {
        var establishment = Establishment.newBuilder()
                                         .setSigningThreshold(toSigningThreshold(specification.getSigningThreshold()))
                                         .addAllKeys(specification.getKeys().stream().map(k -> bs(k))
                                                                  .collect(Collectors.toList()))
                                         .setNextKeysDigest((specification.getNextKeys() == null ? Digest.NONE
                                                                                                 : specification.getNextKeys()).toDigeste())
                                         .setWitnessThreshold(specification.getWitnessThreshold());
        var header = Header.newBuilder().setSequenceNumber(specification.getSequenceNumber())
                           .setVersion(toVersion(specification.getVersion()).setFormat(specification.getFormat()
                                                                                                    .name()))
                           .setPriorEventDigest(specification.getPriorEventDigest().toDigeste())
                           .setIdentifier(identifier.toIdent()).setIlk(ROTATION_TYPE);

        return RotationSpec.newBuilder().setHeader(header).setEstablishment(establishment)
                           .addAllWitnessesAdded(specification.getAddedWitnesses().stream().map(i -> i.toIdent())
                                                              .collect(Collectors.toList()))
                           .addAllWitnessesRemoved(specification.getRemovedWitnesses().stream().map(i -> i.toIdent())
                                                                .collect(Collectors.toList()))
                           .build();
    }
}
