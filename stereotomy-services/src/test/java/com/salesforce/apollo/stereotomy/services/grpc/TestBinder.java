/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.services.grpc.binder.BinderClient;
import com.salesforce.apollo.stereotomy.services.grpc.binder.BinderServer;
import com.salesforce.apollo.stereotomy.services.proto.ProtoBinder;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestBinder {

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
    public void bind() throws Exception {
        var context = DigestAlgorithm.DEFAULT.getOrigin();
        var prefix = UUID.randomUUID().toString();

        var serverMember = new SigningMemberImpl(Utils.getMember(0));
        var clientMember = new SigningMemberImpl(Utils.getMember(1));

        var builder = ServerConnectionCache.newBuilder();
        serverRouter = new LocalRouter(prefix, serverMember, builder, ForkJoinPool.commonPool());
        clientRouter = new LocalRouter(prefix, clientMember, builder, ForkJoinPool.commonPool());

        serverRouter.start();
        clientRouter.start();

        ProtoBinder protoService = new ProtoBinder() {

            @Override
            public CompletableFuture<Boolean> bind(Binding binding) {
                CompletableFuture<Boolean> f = new CompletableFuture<>();
                f.complete(true);
                return f;
            }

            @Override
            public CompletableFuture<Boolean> unbind(Ident identifier) {
                CompletableFuture<Boolean> f = new CompletableFuture<>();
                f.complete(true);
                return f;
            }
        };

        var ci = new ClientIdentity() {

            @Override
            public Digest getFrom() {
                return DigestAlgorithm.DEFAULT.getOrigin();
            }

            @Override
            public Certificate[] getCerts() {
                return null;
            }

            @Override
            public X509Certificate getCert() {
                return null;
            }
        };
        serverRouter.create(serverMember, context, protoService, r -> new BinderServer(ci, null, r), null, null);

        var clientComms = clientRouter.create(clientMember, context, protoService, r -> new BinderServer(ci, null, r),
                                              BinderClient.getCreate(context, null), null);

        var client = clientComms.apply(serverMember, clientMember);

        assertTrue(client.bind(Binding.getDefaultInstance()).get());
        assertTrue(client.unbind(Ident.getDefaultInstance()).get());
    }
}
