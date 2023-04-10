/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.nio.ByteBuffer;
import java.util.function.Function;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;

import com.google.protobuf.Message;

/**
 * @author hal.hildebrand
 *
 */
public final class ProtobufDatatype<Type extends Message> extends BasicDataType<Type> {
    private Function<byte[], Type> factory;

    private ProtobufDatatype(Function<byte[], Type> factory) {
        this.factory = factory;
    }

    @Override
    public Type[] createStorage(int size) {
        @SuppressWarnings("unchecked")
        final var storage = (Type[]) new Object[size];
        return storage;
    }

    @Override
    public int getMemory(Type data) {
        return data.getSerializedSize();
    }

    @Override
    public Type read(ByteBuffer buff) {
        int size = DataUtils.readVarInt(buff);
        byte[] data = new byte[size];
        buff.get(data);
        return factory.apply(data);
    }

    @Override
    public void write(WriteBuffer buff, Type data) {
        buff.putVarInt(data.getSerializedSize());
        buff.put(data.toByteString().toByteArray());
    }
}
