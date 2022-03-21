/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.stereotomy.services.grpc.validation.EventValidationClient;
import com.salesforce.apollo.stereotomy.services.grpc.validation.EventValidationServer;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventValidation;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestEventValidation {

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
    public void validation() throws Exception {
        var context = DigestAlgorithm.DEFAULT.getOrigin();
        var prefix = UUID.randomUUID().toString();

        var serverMember = new SigningMemberImpl(Utils.getMember(0));
        var clientMember = new SigningMemberImpl(Utils.getMember(1));

        var builder = ServerConnectionCache.newBuilder();
        serverRouter = new LocalRouter(prefix, serverMember, builder, ForkJoinPool.commonPool());
        clientRouter = new LocalRouter(prefix, clientMember, builder, ForkJoinPool.commonPool());

        serverRouter.start();
        clientRouter.start();

        ProtoEventValidation protoService = new ProtoEventValidation() {

            @Override
            public CompletableFuture<Boolean> validate(KeyEvent_ event) {
                var f = new CompletableFuture<Boolean>();
                f.complete(true);
                return f;
            }
        };

        serverRouter.create(serverMember, context, protoService, r -> new EventValidationServer(null, r), null, null);

        var clientComms = clientRouter.create(clientMember, context, protoService,
                                              r -> new EventValidationServer(null, r),
                                              EventValidationClient.getCreate(context, null), null);

        var client = clientComms.apply(serverMember, clientMember);

        assertTrue(client.validate(KeyEvent_.getDefaultInstance()).get());
    }
}
