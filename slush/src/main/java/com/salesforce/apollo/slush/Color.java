/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
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
