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
import com.salesforce.apollo.crypto.Digest;
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

    Context.Key<Digest>  CLIENT_CLIENT_ID_KEY   = Context.key("com.salesforce.apollo.archipeligo.from.id.client");
    Metadata.Key<String> METADATA_CLIENT_ID_KEY = Metadata.Key.of("com.salesforce.apollo.archipeligo.from.id",
                                                                  Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> METADATA_CONTEXT_KEY   = Metadata.Key.of("com.salesforce.apollo.archipeligo.context.id",
                                                                  Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> METADATA_TARGET_KEY    = Metadata.Key.of("com.salesforce.apollo.archipeligo.to.id",
                                                                  Metadata.ASCII_STRING_MARSHALLER);
    Context.Key<Digest>  SERVER_CLIENT_ID_KEY   = Context.key("com.salesforce.apollo.archipeligo.from.id.server");
    Context.Key<Digest>  SERVER_CONTEXT_KEY     = Context.key("com.salesforce.apollo.archipeligo.context.id.server");
    Context.Key<Digest>  SERVER_TARGET_KEY      = Context.key("com.salesforce.apollo.archipeligo.to.id.server");

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
