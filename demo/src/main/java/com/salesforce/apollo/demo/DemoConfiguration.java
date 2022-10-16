/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import com.salesforce.apollo.choam.Parameters;

import io.dropwizard.core.Configuration;

/**
 * @author hal.hildebrand
 *
 */
public class DemoConfiguration extends Configuration {
    private Parameters.Builder params = Parameters.newBuilder();

    protected Parameters.Builder getParams() {
        return params;
    }

    protected void setParams(Parameters.Builder params) {
        this.params = params;
    }
}
