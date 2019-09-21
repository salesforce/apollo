/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.web;

import com.salesforce.apollo.ApolloConfiguration;

import io.dropwizard.Configuration;

/**
 * @author hhildebrand
 */
public class ApolloServiceConfiguration extends Configuration {
    String name;

    private ApolloConfiguration apollo = new ApolloConfiguration();

    public ApolloConfiguration getApollo() {
        return apollo;
    }

    public void setApollo(ApolloConfiguration apollo) {
        this.apollo = apollo;
    }

}
