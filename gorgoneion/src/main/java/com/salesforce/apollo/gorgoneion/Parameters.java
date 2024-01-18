/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesforce.apollo.stereotomy.KERL;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Predicate;

/**
 * @author hal.hildebrand
 */
public record Parameters(Clock clock, Duration registrationTimeout, Duration frequency, DigestAlgorithm digestAlgorithm,
                         Duration maxDuration, KERL kerl) {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final static Predicate<SignedAttestation> defaultVerifier;

        static {
            defaultVerifier = x -> true;
        }

        private Clock           clock               = Clock.systemUTC();
        private DigestAlgorithm digestAlgorithm     = DigestAlgorithm.DEFAULT;
        private Duration        frequency           = Duration.ofMillis(5);
        private KERL            kerl;
        private Duration        maxDuration         = Duration.ofSeconds(30);
        private Duration        registrationTimeout = Duration.ofSeconds(30);

        public Parameters build() {
            return new Parameters(clock, registrationTimeout, frequency, digestAlgorithm, maxDuration, kerl);
        }

        public Clock getClock() {
            return clock;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public Duration getFrequency() {
            return frequency;
        }

        public Builder setFrequency(Duration frequency) {
            this.frequency = frequency;
            return this;
        }

        public KERL getKerl() {
            return kerl;
        }

        public Builder setKerl(KERL kerl) {
            this.kerl = kerl;
            return this;
        }

        public Duration getMaxDuration() {
            return maxDuration;
        }

        public Builder setMaxDuration(Duration maxDuration) {
            this.maxDuration = maxDuration;
            return this;
        }

        public Duration getRegistrationTimeout() {
            return registrationTimeout;
        }

        public Builder setRegistrationTimeout(Duration registrationTimeout) {
            this.registrationTimeout = registrationTimeout;
            return this;
        }
    }

}
