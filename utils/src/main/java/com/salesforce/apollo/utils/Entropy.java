/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.utils;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PoolUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hal.hildebrand
 *
 */
final public class Entropy {
    private static class BitsStreamEntropyFactory extends BasePooledObjectFactory<BitsStreamGenerator> {
        @Override
        public BitsStreamGenerator create() throws Exception {
            return new MersenneTwister(bitsStreamEntropy.nextLong());
        }

        @Override
        public PooledObject<BitsStreamGenerator> wrap(BitsStreamGenerator random) {
            return new DefaultPooledObject<>(random);
        }
    }

    private static class SecureRandomFactory extends BasePooledObjectFactory<SecureRandom> {
        @Override
        public SecureRandom create() throws Exception {
            return new SecureRandom();
        }

        @Override
        public PooledObject<SecureRandom> wrap(SecureRandom random) {
            return new DefaultPooledObject<>(random);
        }
    }

    private static final SecureRandom              bitsStreamEntropy = new SecureRandom();
    private static ObjectPool<BitsStreamGenerator> bitsStreamPool    = PoolUtils.erodingPool(new GenericObjectPool<>(new BitsStreamEntropyFactory()));
    private static Logger                          log               = LoggerFactory.getLogger(Entropy.class);
    private static ObjectPool<SecureRandom>        secureRandomPool  = PoolUtils.erodingPool(new GenericObjectPool<>(new SecureRandomFactory()));

    public static void acceptBitsStream(Consumer<BitsStreamGenerator> c) {
        BitsStreamGenerator entropy;
        try {
            entropy = bitsStreamPool.borrowObject();
        } catch (Exception e) {
            log.error("Unable to borrow bitsStream random", e);
            throw new IllegalStateException("Unable to borrow bitsStream random", e);
        }
        try {
            c.accept(entropy);
        } finally {
            try {
                bitsStreamPool.returnObject(entropy);
            } catch (Exception e) {
                log.error("Unable to return bitsStream random", e);
            }
        }
    }

    public static void acceptSecure(Consumer<SecureRandom> c) {
        SecureRandom entropy;
        try {
            entropy = secureRandomPool.borrowObject();
        } catch (Exception e) {
            log.error("Unable to borrow secure random", e);
            throw new IllegalStateException("Unable to borrow secure random", e);
        }
        try {
            c.accept(entropy);
        } finally {
            try {
                secureRandomPool.returnObject(entropy);
            } catch (Exception e) {
                log.error("Unable to return secure random", e);
            }
        }
    }

    public static <T> T applyBitsStream(Function<BitsStreamGenerator, T> func) {
        BitsStreamGenerator entropy;
        try {
            entropy = bitsStreamPool.borrowObject();
        } catch (Exception e) {
            log.error("Unable to borrow bitsStream random", e);
            throw new IllegalStateException("Unable to borrow bitsStream random", e);
        }
        try {
            return func.apply(entropy);
        } finally {
            try {
                bitsStreamPool.returnObject(entropy);
            } catch (Exception e) {
                log.error("Unable to return bitsStream random", e);
            }
        }
    }

    public static <T> T applySecure(Function<SecureRandom, T> func) {
        SecureRandom entropy;
        try {
            entropy = secureRandomPool.borrowObject();
        } catch (Exception e) {
            log.error("Unable to borrow secure random", e);
            throw new IllegalStateException("Unable to borrow secure random", e);
        }
        try {
            return func.apply(entropy);
        } finally {
            try {
                secureRandomPool.returnObject(entropy);
            } catch (Exception e) {
                log.error("Unable to return secure random", e);
            }
        }
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
