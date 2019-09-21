/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
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
    private static final String DEFAULT_AVALANCHE_VAR = "AVALANCHE";
    private static final String DEFAULT_CNC_ENDPOINT_VAR = "CNC_ENDPOINT";
    private static final String DEFAULT_FIREFLIES_VAR = "FIREFLIES";
    private static final String DEFAULT_GHOST_VAR = "GHOST";
    private static final String DEFAULT_HOSTNAME_VAR = "HOST";

    public String avalanchePortVar = DEFAULT_AVALANCHE_VAR;
    public String endpointUrlVar = DEFAULT_CNC_ENDPOINT_VAR;
    public String firefilesPortVar = DEFAULT_FIREFLIES_VAR;
    public String ghostPortVar = DEFAULT_GHOST_VAR;
    public String hostNameVar = DEFAULT_HOSTNAME_VAR;

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
                                            forSigning(pair.getPrivate(), new SecureRandom()), getHostName(),
                                            getFirefliesPort(),
                                            getGhostPort(), getAvalanchePort(), retryPeriod, retries);
        return new MappingIdentitySource(bootstrap, pair.getPrivate(), getHostName(), firefliesPort, ghostPort,
                                         avalanchePort);
    }

    @Override
    protected int getAvalanchePort() {
        return Integer.parseInt(System.getenv(avalanchePortVar));
    }

    @Override
    protected URI getEndpoint() throws URISyntaxException {
        String port = System.getenv(endpointUrlVar);
        return port == null ? getEndpoint() : new URI(port);
    }

    @Override
    protected int getFirefliesPort() {
        String port = System.getenv(firefilesPortVar);
        return port == null ? super.getFirefliesPort() : Integer.parseInt(port);
    }

    @Override
    protected int getGhostPort() {
        String port = System.getenv(ghostPortVar);
        return port == null ? super.getGhostPort() : Integer.parseInt(port);
    }

    @Override
    protected String getHostName() {
        if (hostNameVar != null) {
            String host = System.getenv(hostNameVar);
            if (host != null) { return host; }
        }
        return super.getHostName();
    }

}
