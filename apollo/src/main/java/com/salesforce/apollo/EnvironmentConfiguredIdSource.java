/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.security.SecureRandom;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import com.salesforce.apollo.IdentitySource.MappingIdentitySource;
import com.salesforce.apollo.bootstrap.client.Bootstrap;

/**
 * @author hhildebrand
 */
public class EnvironmentConfiguredIdSource extends BootstrapIdSource {
    private static final String DEFAULT_GRPC_VAR         = "GRPC";
    private static final String DEFAULT_CNC_ENDPOINT_VAR = "CNC_ENDPOINT";
    private static final String DEFAULT_HOSTNAME_VAR     = "HOST";

    public String endpointUrlVar = DEFAULT_CNC_ENDPOINT_VAR;
    public String hostNameVar    = DEFAULT_HOSTNAME_VAR;
    public String grpcPortVar    = DEFAULT_GRPC_VAR;

    @Override
    public IdentitySource getIdentitySource(String caAlias, String identityAlias) {
        final KeyPair pair = generateKeyPair();
        Client client = ClientBuilder.newClient();
        URI uri;
        try {
            uri = getEndpoint();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot convert endpoint to URI: " + endpoint.toExternalForm());
        }
        WebTarget targetEndpoint = client.target(uri);

        Bootstrap bootstrap = new Bootstrap(targetEndpoint, pair.getPublic(),
                forSigning(pair.getPrivate(), new SecureRandom()), getHostName(), grpcPort, retryPeriod, retries);
        return new MappingIdentitySource(bootstrap, pair.getPrivate());
    }

    @Override
    protected URI getEndpoint() throws URISyntaxException {
        String port = System.getenv(endpointUrlVar);
        return port == null ? getEndpoint() : new URI(port);
    }

    @Override
    protected int getGrpcPort() {
        String port = System.getenv(grpcPortVar);
        return port == null ? super.getGrpcPort() : Integer.parseInt(port);
    }

    @Override
    protected String getHostName() {
        if (hostNameVar != null) {
            String host = System.getenv(hostNameVar);
            if (host != null) {
                return host;
            }
        }
        return super.getHostName();
    }

}
