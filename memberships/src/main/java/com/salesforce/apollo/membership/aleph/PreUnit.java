/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public interface PreUnit {
    public record DecodedId(int height, short creator, int epoch) {}

    static DecodedId decode(long id) {
        var height = (int) (id & (1 << 16 - 1));
        id >>= 16;
        var creator = (short) (id & (1 << 16 - 1));
        return new DecodedId(height, creator, (int) (id >> 16));
    }

    static long id(int height, short creator, int epoch) {
        var result = (long) height;
        result += ((long) creator) << 16;
        result += ((long) epoch) << 32;
        return result;
    }

    short creator();

    Any data();

    default boolean dealing() {
        return height() == 0;
    }

    int epoch();

    default boolean equals(PreUnit v) {
        return creator() == v.creator() && height() == v.height() && epoch() == v.epoch();
    }

    Digest hash();

    int height();

    default long id() {
        return id(height(), creator(), epoch());
    }

    default String nickName() {
        return hash().toString();
    }

    byte[] randomSourceData();

    JohnHancock signature();

    Crown view();
}
