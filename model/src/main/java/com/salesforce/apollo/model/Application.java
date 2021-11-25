/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.util.ArrayList;
import java.util.List;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class Application {
    private final Digest               id;
    private final List<NetworkEndpoint> endpoints = new ArrayList<>();

    public Application(Digest id) {
        this.id = id;
    }
}
