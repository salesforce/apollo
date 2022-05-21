/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverClient;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverServer;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestEventObserver {

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
    public void observer() throws Exception {
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

        ProtoEventObserver protoService = new ProtoEventObserver() {

            @Override
            public CompletableFuture<List<AttachmentEvent>> publish(KERL_ kerl) {
                CompletableFuture<List<AttachmentEvent>> f = new CompletableFuture<>();
                f.complete(Collections.emptyList());
                return f;
            }

            @Override
            public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.complete(null);
                return f;
            }

            @Override
            public CompletableFuture<List<AttachmentEvent>> publishEvents(List<KeyEvent_> events) {
                CompletableFuture<List<AttachmentEvent>> f = new CompletableFuture<>();
                f.complete(Collections.emptyList());
                return f;
            }
        };

        serverRouter.create(serverMember, context, protoService, r -> new EventObserverServer(null, r), null, null);

        var clientComms = clientRouter.create(clientMember, context, protoService,
                                              r -> new EventObserverServer(null, r),
                                              EventObserverClient.getCreate(context, null), null);

        var client = clientComms.apply(serverMember, clientMember);

        client.publishAttachments(Collections.emptyList()).get();
        client.publish(KERL_.getDefaultInstance()).get();
        client.publishEvents(Collections.emptyList()).get();
    }
}
