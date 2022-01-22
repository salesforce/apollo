/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import static org.junit.Assert.assertNotNull;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;

/**
 * @author hal.hildebrand
 *
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class DemoTest {
    @ClassRule
    private static DropwizardAppExtension<DemoConfiguration> EXT = new DropwizardAppExtension<>(DemoApplication.class,
                                                                                                ResourceHelpers.resourceFilePath("demo-test.yaml"));

    @Test
    void smokin() {
        Client client = EXT.client();

        Response response = client.target(String.format("http://localhost:%d/login", EXT.getLocalPort()))
                                  .request()
                                  .post(Entity.json(testQuery()));
        assertNotNull(response);

    }

    private Object testQuery() {
        // TODO Auto-generated method stub
        return null;
    }

}
