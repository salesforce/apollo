/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import io.grpc.BindableService;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author hal.hildebrand
 */
public interface Router {

    void close(Duration await);

    <Client extends Link, Service extends Router.ServiceRouting> CommonCommunications<Client, Service> create(
    Member member, Digest context, Service service, Function<RoutableService<Service>, BindableService> factory,
    CreateClientCommunications<Client> createFunction, Client localLoopback);

    <Service, Client extends Link> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                Service service, String routingLabel,
                                                                                Function<RoutableService<Service>, BindableService> factory);

    <Service, Client extends Link> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                Service service, String routingLabel,
                                                                                Function<RoutableService<Service>, BindableService> factory,
                                                                                Predicate<FernetServerInterceptor.HashedToken> validator);

    <Client extends Link, Service> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                Service service, String routingLabel,
                                                                                Function<RoutableService<Service>, BindableService> factory,
                                                                                CreateClientCommunications<Client> createFunction,
                                                                                Client localLoopback);

    <Client extends Link, Service> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                Service service, String routingLabel,
                                                                                Function<RoutableService<Service>, BindableService> factory,
                                                                                CreateClientCommunications<Client> createFunction,
                                                                                Client localLoopback,
                                                                                Predicate<FernetServerInterceptor.HashedToken> validator);

    ClientIdentity getClientIdentityProvider();

    Member getFrom();

    void start();

    @FunctionalInterface
    interface ClientConnector<Client> {
        Client connect(Member to);
    }

    interface ServiceRouting {
        default String routing() {
            return getClass().getCanonicalName();
        }
    }

}
