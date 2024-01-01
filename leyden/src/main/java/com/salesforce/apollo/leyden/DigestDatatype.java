/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.leyden;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;

import java.nio.ByteBuffer;

/**
 * @author hal.hildebrand
 */
public final class DigestDatatype extends BasicDataType<Digest> {
    private final DigestAlgorithm algorithm;

    public DigestDatatype(DigestAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public int compare(Digest a, Digest b) {
        return a.compareTo(b);
    }

    @Override
    public Digest[] createStorage(int size) {
        return new Digest[size];
    }

    @Override
    public int getMemory(Digest data) {
        return algorithm.longLength() * 8;
    }

    @Override
    public Digest read(ByteBuffer buff) {
        byte[] data = new byte[algorithm.longLength() * 8];
        buff.get(data);
        return new Digest(algorithm, data);
    }

    @Override
    public void write(WriteBuffer buff, Digest data) {
        buff.put(data.getBytes());
    }
}
