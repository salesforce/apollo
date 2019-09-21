/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */

package com.salesforce.apollo.bootstrap;

import com.codahale.metrics.health.HealthCheck;

/**
 * @author hhildebrand
 */
public class BoostrapCaHealthCheck extends HealthCheck {

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
