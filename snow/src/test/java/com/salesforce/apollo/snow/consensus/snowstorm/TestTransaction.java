/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.salesforce.apollo.snow.choices.TestDecidable;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class TestTransaction extends TestDecidable implements Tx {
    public byte[]    bytesV;
    public Set<Tx>   dependenciesV = new HashSet<>();
    public Set<ID>   inputIDsV     = new HashSet<>();
    public Throwable verifyV;

    public byte[] bytes() {
        return bytesV;
    }

    public Collection<Tx> dependencies() {
        return dependenciesV == null ? Collections.emptySet() : dependenciesV;
    }

    public Throwable getVerifyV() {
        return verifyV;
    }

    public Set<ID> inputIDs() {
        return inputIDsV;
    }

    public void setVerifyV(Throwable verifyV) {
        this.verifyV = verifyV;
    }

    public void verify() {

    }
}
