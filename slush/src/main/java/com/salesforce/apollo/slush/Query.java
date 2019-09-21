/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import java.util.function.Supplier;

/**
 * A simple holder Pojo because containment typing
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Query<T> implements Supplier<T> {
    public T value;

    public Query() {}

    public Query(T value) {
        this.value = value;
    }

    @Override
    public T get() {
        return value;
    }
}
