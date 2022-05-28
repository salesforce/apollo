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
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;

/**
 * @author hal.hildebrand
 *
 */
final public class Entropy {

    private static ThreadLocal<BitsStreamGenerator> bitsStreamPool   = new ThreadLocal<>() {

                                                                         @Override
                                                                         protected BitsStreamGenerator initialValue() {
                                                                             return new MersenneTwister(secureEntropy.nextLong());
                                                                         }
                                                                     };
    private static final SecureRandom               secureEntropy;
    private static ThreadLocal<SecureRandom>        secureRandomPool = new ThreadLocal<>() {

                                                                         @Override
                                                                         protected SecureRandom initialValue() {
                                                                             SecureRandom entropy;
                                                                             try {
                                                                                 entropy = SecureRandom.getInstance("SHA1PRNG");
                                                                             } catch (NoSuchAlgorithmException e) {
                                                                                 throw new IllegalStateException(e);
                                                                             }
                                                                             entropy.setSeed(secureEntropy.nextLong());
                                                                             return entropy;
                                                                         }
                                                                     };

    static {
        try {
            secureEntropy = SecureRandom.getInstanceStrong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void acceptBitsStream(Consumer<BitsStreamGenerator> c) {
        c.accept(bitsStreamPool.get());
    }

    public static void acceptSecure(Consumer<SecureRandom> c) {
        c.accept(secureRandomPool.get());
    }

    public static <T> T applyBitsStream(Function<BitsStreamGenerator, T> func) {
        return func.apply(bitsStreamPool.get());
    }

    public static <T> T applySecure(Function<SecureRandom, T> func) {
        return func.apply(secureRandomPool.get());
    }

    public static void nextBitsStreamBytes(byte[] bytes) {
        acceptBitsStream(entropy -> entropy.nextBytes(bytes));
    }

    public static int nextBitsStreamInt() {
        return applyBitsStream(entropy -> entropy.nextInt());
    }

    public static int nextBitsStreamInt(int range) {
        return applyBitsStream(entropy -> entropy.nextInt(range));
    }

    public static long nextBitsStreamLong() {
        return applyBitsStream(entropy -> entropy.nextLong());
    }

    public static long nextBitsStreamLong(long range) {
        return applyBitsStream(entropy -> entropy.nextLong(range));
    }

    public static void nextSecureBytes(byte[] bytes) {
        acceptSecure(entropy -> entropy.nextBytes(bytes));
    }

    public static int nextSecureInt() {
        return applySecure(entropy -> entropy.nextInt());
    }

    public static int nextSecureInt(int range) {
        return applySecure(entropy -> entropy.nextInt(range));
    }

    public static long nextSecureLong() {
        return applySecure(entropy -> entropy.nextLong());
    }

    public static long nextSecureLong(long range) {
        return applySecure(entropy -> entropy.nextLong(range));
    }

    public static void secureShuffle(List<?> list) {
        acceptSecure(entropy -> Collections.shuffle(list, entropy));
    }

    private Entropy() {
    }
}
