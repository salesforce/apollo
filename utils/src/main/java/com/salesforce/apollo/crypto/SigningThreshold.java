/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.math3.fraction.Fraction;

import com.salesforce.apollo.crypto.SigningThreshold.Weighted.Weight;
import com.salesforce.apollo.crypto.SigningThreshold.Weighted.WeightedImpl;
import com.salesforce.apollo.crypto.SigningThreshold.Weighted.Weight.WeightImpl;

/**
 * @author hal.hildebrand
 *
 */
public interface SigningThreshold {

    interface Unweighted extends SigningThreshold {

        int getThreshold();

    }

    interface Weighted extends SigningThreshold {

        interface Weight {
            class WeightImpl implements Weight {
                private final Integer denominator;
                private final Integer numerator;

                public WeightImpl(Integer numerator, Integer denominator) {
                    this.numerator = numerator;
                    this.denominator = denominator;
                }

                @Override
                public Optional<Integer> denominator() {
                    return Optional.ofNullable(denominator);
                }

                @Override
                public int numerator() {
                    return numerator;
                }

                @Override
                public int hashCode() {
                    return Objects.hash(denominator, numerator);
                }

                @Override
                public boolean equals(Object obj) {
                    if (this == obj) {
                        return true;
                    }
                    if (!(obj instanceof WeightImpl)) {
                        return false;
                    }
                    WeightImpl other = (WeightImpl) obj;
                    return Objects.equals(denominator, other.denominator) && Objects.equals(numerator, other.numerator);
                }

            }

            Optional<Integer> denominator();

            int numerator();
        }

        class WeightedImpl implements Weighted {
            private final Weight[][] weights;

            public WeightedImpl(Weight[][] weightGroups) {
                this.weights = weightGroups;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (!(obj instanceof WeightedImpl)) {
                    return false;
                }
                WeightedImpl other = (WeightedImpl) obj;
                return Arrays.deepEquals(weights, other.weights);
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + Arrays.deepHashCode(weights);
                return result;
            }

            @Override
            public Weight[][] getWeights() {
                return weights;
            }
        }

        Weight[][] getWeights();
    }

    public static int countWeights(Weight[][] weights) {
        return Arrays.stream(weights).mapToInt(w -> w.length).sum();
    }

    public static Weight[] group(String... weights) {
        return Stream.of(weights).map(SigningThreshold::weight).toArray(Weight[]::new);
    }

    public static Weight[] group(Weight... weights) {
        return weights;
    }

    public static boolean thresholdMet(SigningThreshold threshold, int[] indexes) {
        if (threshold instanceof SigningThreshold.Unweighted) {
            return thresholdMet((SigningThreshold.Unweighted) threshold, indexes);
        } else if (threshold instanceof SigningThreshold.Weighted) {
            return thresholdMet((SigningThreshold.Weighted) threshold, indexes);
        } else {
            throw new IllegalArgumentException("Unknown threshold type: " + threshold.getClass().getCanonicalName());
        }
    }

    public static boolean thresholdMet(SigningThreshold.Unweighted threshold, int[] indexes) {
        requireNonNull(indexes, "indexes");
        return indexes.length >= threshold.getThreshold();
    }

    public static boolean thresholdMet(SigningThreshold.Weighted threshold, int[] indexes) {
        requireNonNull(indexes);

        if (indexes.length == 0) {
            return false;
        }

        var maxIndex = IntStream.of(indexes).max().getAsInt();
        var countWeights = countWeights(threshold.getWeights());

        var sats = prefillSats(Integer.max(maxIndex + 1, countWeights));
        for (var i : indexes) {
            sats[i] = true;
        }

        var index = 0;
        for (var clause : threshold.getWeights()) {
            var accumulator = Fraction.ZERO;
            for (var weight : clause) {
                if (sats[index]) {
                    accumulator = accumulator.add(fraction(weight));
                }
                index++;
            }

            if (accumulator.compareTo(Fraction.ONE) < 0) {
                return false;
            }
        }

        return true;
    }

    public static SigningThreshold.Unweighted unweighted(int threshold) {
        if (threshold <= 0) {
            throw new IllegalArgumentException("threshold must be greater than 0");
        }

        return new Unweighted() {
            @Override
            public int getThreshold() {
                return threshold;
            }
        };
    }

    public static Weight weight(int value) {
        return weight(value, null);
    }

    public static Weight weight(int numerator, Integer denominator) {
        if (denominator != null && denominator <= 0) {
            throw new IllegalArgumentException("denominator must be > 0");
        }

        if (numerator <= 0) {
            throw new IllegalArgumentException("numerator must be > 0");
        }

        return new WeightImpl(numerator, denominator);
    }

    public static Weight weight(String value) {
        var parts = value.split("/");
        if (parts.length == 1) {
            return weight(Integer.parseInt(parts[0]));
        } else if (parts.length == 2) {
            return weight(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        } else {
            throw new IllegalArgumentException("invalid weight: " + value);
        }
    }

    public static SigningThreshold.Weighted weighted(String... weightsAsStrings) {
        var weights = Stream.of(weightsAsStrings).map(SigningThreshold::weight).toArray(Weight[]::new);

        return weighted(weights);
    }

    public static SigningThreshold.Weighted weighted(Weight... weights) {
        return weighted(new Weight[][] { weights });
    }

    public static SigningThreshold.Weighted weighted(Weight[]... weightGroups) {
        for (var group : weightGroups) {
            if (!sumGreaterThanOrEqualToOne(group)) {
                throw new IllegalArgumentException("group sum is less than 1: " + Arrays.deepToString(group));
            }
        }
        return new WeightedImpl(weightGroups);
    }

    private static Fraction fraction(Weight weight) {
        if (weight.denominator().isEmpty()) {
            return new Fraction(weight.numerator());
        }

        return new Fraction(weight.numerator(), weight.denominator().get());
    }

    private static boolean[] prefillSats(int count) {
        var sats = new boolean[count];
        Arrays.fill(sats, false);
        return sats;
    }

    private static boolean sumGreaterThanOrEqualToOne(Weight[] weights) {
        var sum = Fraction.ZERO;
        for (var w : weights) {
            // noinspection ObjectAllocationInLoop
            sum = sum.add(fraction(w));
        }

        return sum.compareTo(Fraction.ONE) >= 0;
    }
}
