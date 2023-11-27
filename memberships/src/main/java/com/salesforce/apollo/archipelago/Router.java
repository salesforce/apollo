/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import java.time.Duration;
import java.util.function.Function;

import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Metadata;

/**
 * @author hal.hildebrand
 *
 */
public interface Router {

    @FunctionalInterface
    interface ClientConnector<Client> {
        Client connect(Member to);
    }

    interface ServiceRouting {
        default String routing() {
            return getClass().getCanonicalName();
        }
    }

    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_AGENT_ID          = "agent.id";
    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_AGENT_ID_SERVER   = "agent.id.server";
    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_CONTEXT_ID        = "context.id";
    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_CONTEXT_ID_SERVER = "context.id.server";
    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_FROM_ID           = "from.id";
    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_FROM_ID_CLIENT    = "from.id.client";
    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_FROM_ID_SERVER    = "from.id.server";
    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_TO_ID             = "to.id";
    String               COM_SALESFORCE_APOLLO_ARCHIPELIGO_TO_ID_SERVER      = "to.id.server";
    Context.Key<Digest>  CLIENT_CLIENT_ID_KEY                                = Context.key(COM_SALESFORCE_APOLLO_ARCHIPELIGO_FROM_ID_CLIENT);
    Metadata.Key<String> METADATA_AGENT_KEY                                  = Metadata.Key.of(COM_SALESFORCE_APOLLO_ARCHIPELIGO_AGENT_ID,
                                                                                               Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> METADATA_CLIENT_ID_KEY                              = Metadata.Key.of(COM_SALESFORCE_APOLLO_ARCHIPELIGO_FROM_ID,
                                                                                               Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> METADATA_CONTEXT_KEY                                = Metadata.Key.of(COM_SALESFORCE_APOLLO_ARCHIPELIGO_CONTEXT_ID,
                                                                                               Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> METADATA_TARGET_KEY                                 = Metadata.Key.of(COM_SALESFORCE_APOLLO_ARCHIPELIGO_TO_ID,
                                                                                               Metadata.ASCII_STRING_MARSHALLER);
    Context.Key<Digest>  SERVER_AGENT_ID_KEY                                 = Context.key(COM_SALESFORCE_APOLLO_ARCHIPELIGO_AGENT_ID_SERVER);
    Context.Key<Digest>  SERVER_CLIENT_ID_KEY                                = Context.key(COM_SALESFORCE_APOLLO_ARCHIPELIGO_FROM_ID_SERVER);
    Context.Key<Digest>  SERVER_CONTEXT_KEY                                  = Context.key(COM_SALESFORCE_APOLLO_ARCHIPELIGO_CONTEXT_ID_SERVER);
    Context.Key<Digest>  SERVER_TARGET_KEY                                   = Context.key(COM_SALESFORCE_APOLLO_ARCHIPELIGO_TO_ID_SERVER);

    void close(Duration await);

    <Client extends Link, Service extends Router.ServiceRouting> CommonCommunications<Client, Service> create(Member member,
                                                                                                              Digest context,
                                                                                                              Service service,
                                                                                                              Function<RoutableService<Service>, BindableService> factory,
                                                                                                              CreateClientCommunications<Client> createFunction,
                                                                                                              Client localLoopback);

    <Service, Client extends Link> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                Service service, String routingLabel,
                                                                                Function<RoutableService<Service>, BindableService> factory);

    <Client extends Link, Service> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                Service service, String routingLabel,
                                                                                Function<RoutableService<Service>, BindableService> factory,
                                                                                CreateClientCommunications<Client> createFunction,
                                                                                Client localLoopback);

    ClientIdentity getClientIdentityProvider();

    Member getFrom();

    void start();

}
