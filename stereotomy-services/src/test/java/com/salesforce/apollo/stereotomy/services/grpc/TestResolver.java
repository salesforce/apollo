/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.stereotomy.services.grpc.resolver.ResolverClient;
import com.salesforce.apollo.stereotomy.services.grpc.resolver.ResolverServer;
import com.salesforce.apollo.stereotomy.services.proto.ProtoResolver;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestResolver {

    private LocalRouter clientRouter;
    private LocalRouter serverRouter;

    @AfterEach
    public void after() {
        if (serverRouter != null) {
            serverRouter.close();
            serverRouter = null;
        }
        if (clientRouter != null) {
            clientRouter.close();
            clientRouter = null;
        }
    }

    @Test
    public void resolver() throws Exception {
        var context = DigestAlgorithm.DEFAULT.getOrigin();
        var prefix = UUID.randomUUID().toString();

        var serverMember = new SigningMemberImpl(Utils.getMember(0));
        var clientMember = new SigningMemberImpl(Utils.getMember(1));

        var builder = ServerConnectionCache.newBuilder();
        final var exec = Executors.newFixedThreadPool(3);
        serverRouter = new LocalRouter(prefix, builder, exec, null);
        clientRouter = new LocalRouter(prefix, builder, exec, null);

        serverRouter.setMember(serverMember);
        clientRouter.setMember(clientMember);

        serverRouter.start();
        clientRouter.start();

        ProtoResolver protoService = new ProtoResolver() {

            @Override
            public Optional<Binding> lookup(Ident prefix) {
                return Optional.of(Binding.getDefaultInstance());
            }
        };

        serverRouter.create(serverMember, context, protoService, r -> new ResolverServer(r, exec, null), null, null);

        var clientComms = clientRouter.create(clientMember, context, protoService,
                                              r -> new ResolverServer(r, exec, null),
                                              ResolverClient.getCreate(context, null), null);

        var client = clientComms.apply(serverMember, clientMember);

        assertEquals(Binding.getDefaultInstance(), client.lookup(Ident.getDefaultInstance()).get());
    }
}
