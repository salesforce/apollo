/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.google.protobuf.Any;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public record Parameters(Function<SignedAttestation, CompletableFuture<Boolean>> verifier, Clock clock,
                         Duration registrationTimeout, Function<SignedNonce, CompletableFuture<Any>> attester,
                         Duration frequency, DigestAlgorithm digestAlgorithm) {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final static CompletableFuture<Any>     defaultAttester;
        private final static CompletableFuture<Boolean> defaultVerifier;

        static {
            defaultAttester = new CompletableFuture<>();
            defaultAttester.complete(Any.getDefaultInstance());
            defaultVerifier = new CompletableFuture<>();
            defaultVerifier.complete(true);
        }

        private Function<SignedNonce, CompletableFuture<Any>>           attester            = sn -> defaultAttester;
        private Clock                                                   clock               = Clock.systemUTC();
        private DigestAlgorithm                                         digestAlgorithm     = DigestAlgorithm.DEFAULT;
        private Duration                                                frequency           = Duration.ofMillis(30);
        private Duration                                                registrationTimeout = Duration.ofSeconds(30);
        private Function<SignedAttestation, CompletableFuture<Boolean>> verifier            = sa -> defaultVerifier;

        public Parameters build() {
            return new Parameters(verifier, clock, registrationTimeout, attester, frequency, digestAlgorithm);
        }

        public Function<SignedNonce, CompletableFuture<Any>> getAttester() {
            return attester;
        }

        public Clock getClock() {
            return clock;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public Duration getFrequency() {
            return frequency;
        }

        public Duration getRegistrationTimeout() {
            return registrationTimeout;
        }

        public Function<SignedAttestation, CompletableFuture<Boolean>> getVerifier() {
            return verifier;
        }

        public Builder setAttester(Function<SignedNonce, CompletableFuture<Any>> attester) {
            this.attester = attester;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public Builder setFrequency(Duration frequency) {
            this.frequency = frequency;
            return this;
        }

        public Builder setRegistrationTimeout(Duration registrationTimeout) {
            this.registrationTimeout = registrationTimeout;
            return this;
        }

        public Builder setVerifier(Function<SignedAttestation, CompletableFuture<Boolean>> verifier) {
            this.verifier = verifier;
            return this;
        }
    }

}
