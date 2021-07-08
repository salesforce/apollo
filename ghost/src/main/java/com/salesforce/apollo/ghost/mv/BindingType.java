/*
no * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.mv;

import java.nio.ByteBuffer;
import java.nio.channels.UnsupportedAddressTypeException;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.ghost.proto.Binding;

/**
 * @author hal.hildebrand
 *
 */

public class BindingType implements DataType {

    @Override
    public int compare(Object a, Object b) {
        throw new UnsupportedAddressTypeException();
    }

    @Override
    public int getMemory(Object obj) {
        if (obj instanceof Binding b) {
            return b.getSerializedSize();
        }
        throw new IllegalArgumentException();
    }

    @Override
    public Binding read(ByteBuffer buff) {
        try {
            return Binding.parseFrom(buff);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Cannot deserialze Binding", e);
        }
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        if (obj instanceof Binding binding) {
            buff.put(binding.toByteArray());
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }
}
