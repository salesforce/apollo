/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.utils;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;

/**
 * @author hal.hildebrand
 *
 */
final public class Entropy {
    private static ThreadLocal<BitsStreamGenerator> BIT_STREAM_ENTROPY = new ThreadLocal<>() {
        @Override
        protected BitsStreamGenerator initialValue() {
            return new MersenneTwister();
        }
    };

    private static ThreadLocal<SecureRandom> ENTROPY = new ThreadLocal<>() {
        @Override
        protected SecureRandom initialValue() {
            try {
                return SecureRandom.getInstance("SHA1PRNG");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Cannot create secure entropy", e);
            }
        }
    };

    public static void nextBitsStreamBytes(byte[] lobMacSalt) {
        bitStreamEntropy().nextBytes(lobMacSalt);
    }

    public static int nextBitsStreamInt() {
        return bitStreamEntropy().nextInt();
    }

    public static int nextBitsStreamInt(int range) {
        return bitStreamEntropy().nextInt(range);
    }

    public static long nextBitsStreamLong() {
        return bitStreamEntropy().nextLong();
    }

    public static long nextBitsStreamLong(long range) {
        return bitStreamEntropy().nextLong(range);
    }

    public static void nextSecureBytes(byte[] lobMacSalt) {
        secureEntropy().nextBytes(lobMacSalt);
    }

    public static int nextSecureInt() {
        return secureEntropy().nextInt();
    }

    public static int nextSecureInt(int range) {
        return secureEntropy().nextInt(range);
    }

    public static long nextSecureLong() {
        return secureEntropy().nextLong();
    }

    public static long nextSecureLong(long range) {
        return secureEntropy().nextLong(range);
    }

    public static void secureShuffle(List<?> list) {
        Collections.shuffle(list, secureEntropy());
    }

    private static BitsStreamGenerator bitStreamEntropy() {
        return BIT_STREAM_ENTROPY.get();
    }

    private static SecureRandom secureEntropy() {
        return ENTROPY.get();
    }

    private Entropy() {
    }
}
