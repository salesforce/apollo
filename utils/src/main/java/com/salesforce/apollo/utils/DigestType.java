/*
no * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.nio.ByteBuffer;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */

public class DigestType implements DataType {

    @Override
    public int compare(Object a, Object b) {
        return ((Digest) a).compareTo(((Digest) b));
    }

    @Override
    public int getMemory(Object obj) {
        return ((Digest) obj).getAlgorithm().digestLength() + 1;
    }

    @Override
    public Digest read(ByteBuffer buff) {
        return new Digest(buff);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        Digest digest = (Digest) obj;
        buff.put(digest.getAlgorithm().digestCode());
        for (long l : digest.getLongs()) {
            buff.putLong(l);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }
}
