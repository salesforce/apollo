/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.web.resources;

import com.codahale.metrics.health.HealthCheck;
import com.salesforce.apollo.fireflies.View;

public class FirefliesHealthCheck extends HealthCheck {
    @SuppressWarnings("unused")
    private final View view;

    public FirefliesHealthCheck(View view) {
        super();
        this.view = view;
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
