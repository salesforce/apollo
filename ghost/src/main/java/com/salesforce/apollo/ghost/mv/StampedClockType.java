/*
no * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.mv;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.ghost.proto.StampedClock;
import com.salesforce.apollo.utils.bloomFilters.ClockValue;

/**
 * @author hal.hildebrand
 *
 */

public class StampedClockType implements DataType {

    private final Comparator<ClockValue> comparator;

    public StampedClockType(Comparator<ClockValue> comparator) {
        this.comparator = comparator;
    }

    @Override
    public int compare(Object a, Object b) {
        if (a instanceof StampedClock first) {
            if (b instanceof StampedClock second) {
                return comparator.compare(ClockValue.of(first.getClock()), ClockValue.of(first.getClock()));
            }
        }
        throw new IllegalArgumentException("Unknown types");
    }

    @Override
    public int getMemory(Object obj) {
        if (obj instanceof StampedClock clock) {
            return clock.getSerializedSize();
        }
        throw new IllegalArgumentException();
    }

    @Override
    public StampedClock read(ByteBuffer buff) {
        try {
            return StampedClock.parseFrom(buff);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Cannot deserialze Stamped Clock key", e);
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
        if (obj instanceof StampedClock clock) {
            buff.put(clock.toByteArray());
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }
}
