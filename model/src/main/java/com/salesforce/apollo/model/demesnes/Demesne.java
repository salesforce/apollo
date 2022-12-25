/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes;

import java.util.List;

import com.salesforce.apollo.crypto.Digest;

/**
 * Domain Isolate interface
 *
 * @author hal.hildebrand
 *
 */
public interface Demesne {

    boolean active();

    void start();

    void stop();

    void viewChange(Digest viewId, List<Digest> joining, List<Digest> leaving);

}
