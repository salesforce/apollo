/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.web.resources;

import com.codahale.metrics.health.HealthCheck;
import com.salesforce.apollo.ghost.Ghost;

/**
 * @author hhildebrand
 */
public class GhostHealthCheck extends HealthCheck {

    @SuppressWarnings("unused")
    private final Ghost ghost;

    public GhostHealthCheck(Ghost ghost) {
        this.ghost = ghost;
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
