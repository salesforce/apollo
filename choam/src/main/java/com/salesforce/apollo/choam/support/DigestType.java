/*
no * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.salesforce.apollo.cryptography.Digest;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;

import java.nio.ByteBuffer;

/**
 * @author hal.hildebrand
 */

public class DigestType extends BasicDataType<Digest> {

    @Override
    public int compare(Digest a, Digest b) {
        return ((Digest) a).compareTo(((Digest) b));
    }

    @Override
    public Digest[] createStorage(int size) {
        return new Digest[size];
    }

    @Override
    public int getMemory(Digest obj) {
        return ((Digest) obj).getAlgorithm().digestLength() + 1;
    }

    @Override
    public Digest read(ByteBuffer buff) {
        return new Digest(buff);
    }

    @Override
    public void write(WriteBuffer buff, Digest digest) {
        buff.put(digest.getAlgorithm().digestCode());
        for (long l : digest.getLongs()) {
            buff.putLong(l);
        }
    }
}
