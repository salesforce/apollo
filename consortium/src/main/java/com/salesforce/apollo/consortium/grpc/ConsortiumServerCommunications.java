/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.grpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.salesfoce.apollo.consortium.proto.ConsortiumGrpc.ConsortiumImplBase;
import com.salesforce.apollo.comm.grpc.BaseServerCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.ConsortiumMetrics;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumServerCommunications extends ConsortiumImplBase implements BaseServerCommunications<Service> {
    @SuppressWarnings("unused")
    private Service                 system;
    private ClientIdentity          identity;
    private Map<HashKey, Service>   services = new ConcurrentHashMap<>();
    @SuppressWarnings("unused")
    private final ConsortiumMetrics metrics;

    public ConsortiumServerCommunications(Service system, ClientIdentity identity, ConsortiumMetrics metrics) {
        this.metrics = metrics;
        this.system = system;
        this.identity = identity;
    }

    @Override
    public ClientIdentity getClientIdentity() {
        return identity;
    }

    @Override
    public void register(HashKey id, Service service) {
        services.computeIfAbsent(id, m -> service);
    }

}
