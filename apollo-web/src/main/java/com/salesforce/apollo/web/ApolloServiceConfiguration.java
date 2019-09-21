/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
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
