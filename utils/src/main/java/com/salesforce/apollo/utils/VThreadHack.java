/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.concurrent.ThreadFactory;

/**
 * @author hal.hildebrand
 *
 */
public class VThreadHack {

    public static ThreadFactory virtualThreadFactory(String name) {
        return Thread.ofVirtual().name(name).factory();
    }

}
