/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.web.resources;

import com.codahale.metrics.health.HealthCheck;
import com.salesforce.apollo.avalanche.Avalanche;

/**
 * @author hhildebrand
 */
public class AvalancheHealthCheck extends HealthCheck {

    @SuppressWarnings("unused")
    private final Avalanche avalanche;

    public AvalancheHealthCheck(Avalanche avalanche) {
        this.avalanche = avalanche;
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
