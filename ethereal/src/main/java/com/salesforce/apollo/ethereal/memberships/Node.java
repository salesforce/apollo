/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import java.util.function.Consumer;

import com.salesfoce.apollo.ethereal.proto.PreUnit_s;

public interface Node {
    default boolean failed() {
        return true;
    }

    default PreUnit_s getPu() {
        throw new UnsupportedOperationException("Non materialize node");
    }

    default void missing(Consumer<PreUnit_s> c) {
    };
}
