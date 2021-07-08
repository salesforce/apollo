/*
no * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.mv;

import java.nio.ByteBuffer;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.ghost.proto.Content;

/**
 * @author hal.hildebrand
 *
 */

public class ContentType implements DataType {

    @Override
    public int compare(Object a, Object b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMemory(Object obj) {
        if (obj instanceof Content c) {
            return c.getSerializedSize();
        }
        throw new IllegalArgumentException();
    }

    @Override
    public Content read(ByteBuffer buff) {
        try {
            return Content.parseFrom(buff);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Cannot deserialze Content", e);
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
        if (obj instanceof Content content) {
            buff.put(content.toByteArray());
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }
}
