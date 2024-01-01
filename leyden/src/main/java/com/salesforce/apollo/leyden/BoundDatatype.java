/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.leyden;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.leyden.proto.Bound;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;

import java.nio.ByteBuffer;

/**
 * @author hal.hildebrand
 */
public final class BoundDatatype extends BasicDataType<Bound> {

    @Override
    public int compare(Bound a, Bound b) {
        return super.compare(a, b);
    }

    @Override
    public Bound[] createStorage(int size) {
        return new Bound[size];
    }

    @Override
    public int getMemory(Bound data) {
        return data.getSerializedSize();
    }

    @Override
    public Bound read(ByteBuffer buff) {
        int size = DataUtils.readVarInt(buff);
        byte[] data = new byte[size];
        buff.get(data);
        try {
            return Bound.parseFrom(buff);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void write(WriteBuffer buff, Bound data) {
        buff.putVarInt(data.getSerializedSize());
        buff.put(data.toByteString().toByteArray());
    }
}
