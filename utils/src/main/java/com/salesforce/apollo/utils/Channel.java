/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author hal.hildebrand
 *
 * @param <T>
 */
public interface Channel<T> {

    void close();

    void consume(Consumer<List<T>> consumer);

    void consumeEach(Consumer<T> consumer);

    boolean isClosed();

    boolean offer(T element);

    void open();

    int size();

    void submit(T element);

}
