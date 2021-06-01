/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.stereotomy.QualifiedBase64.digest;
import static com.salesforce.apollo.stereotomy.QualifiedBase64.publicKey;

import java.security.PublicKey;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.salesfoce.apollo.stereotomy.event.proto.Establishment;
import com.salesfoce.apollo.stereotomy.event.proto.Header;
import com.salesfoce.apollo.stereotomy.event.proto.Weights;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.SigningThreshold.Weighted.Weight;

/**
 * @author hal.hildebrand
 *
 */
abstract public class EstablishmentEventImpl extends KeyEventImpl implements EstablishmentEvent {

    private final Establishment establishment;

    public EstablishmentEventImpl(Header header, Establishment establishment) {
        super(header);
        this.establishment = establishment;
    }

    @Override
    public List<PublicKey> getKeys() {
        return establishment.getKeysList().stream().map(bs -> publicKey(bs)).collect(Collectors.toList());
    }

    @Override
    public Optional<Digest> getNextKeyConfiguration() {
        return establishment.getNextKeyConfiguration().isEmpty() ? Optional.empty()
                : Optional.of(digest(establishment.getNextKeyConfiguration()));
    }

    @Override
    public SigningThreshold getSigningThreshold() {
        com.salesfoce.apollo.stereotomy.event.proto.SigningThreshold signingThreshold = establishment.getSigningThreshold();
        if (signingThreshold.getWeightsCount() == 0) {
            return new SigningThreshold.Unweighted() {
                @Override
                public int threshold() {
                    return signingThreshold.getThreshold();
                }

            };
        } else {
            return new SigningThreshold.Weighted() {

                @Override
                public Weight[][] weights() {
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
            public int numerator() {
                return w.getNumerator();
            }

            @Override
            public Optional<Integer> denominator() {
                return Optional.of(w.getDenominator());
            }
        };

    }

    @Override
    public int getWitnessThreshold() {
        return establishment.getGetWitnessThreshold();
    }
}
