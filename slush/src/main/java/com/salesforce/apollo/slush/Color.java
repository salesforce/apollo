/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The state of a slush node
 */
public enum Color implements Enumerable<Color> {
    Blue, Red;

    private static final List<Color> ENUMERATION = Arrays.asList(new Color[] { Blue, Red });

    @Override
    public Iterator<Color> enumerate() {
        return ENUMERATION.iterator();
    }

    @Override
    public Color value() {
        return this;
    }
}
