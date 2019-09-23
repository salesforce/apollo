/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.bootstrap;

import io.dropwizard.Configuration;

/**
 * @author hhildebrand
 */
public class BootstrapConfiguration extends Configuration {
    private static final String DEFAULT_CONNECTION = "jdbc:h2:mem:bootstrap";

    public int cardinality = 100;
    /**
     * The JDBC connection URL
     */
    public String dbConnect = DEFAULT_CONNECTION;
    public double faultTolerance = 0.1;
    public double probabilityByzantine = 0.2;

}
