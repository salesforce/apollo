/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */

package com.salesforce.apollo.web.resources;

import com.codahale.metrics.health.HealthCheck;
import com.salesforce.apollo.Apollo;

/**
 * @author hhildebrand
 */
public class ApolloHealthCheck extends HealthCheck {

    @SuppressWarnings("unused")
    private final Apollo apollo;

    public ApolloHealthCheck(Apollo apollo) {
        this.apollo = apollo;
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
