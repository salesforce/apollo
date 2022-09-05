/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserver;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverClient;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverServer;

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
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        var serverMember = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var clientMember = new ControlledIdentifierMember(stereotomy.newIdentifier().get());

        var builder = ServerConnectionCache.newBuilder();
        final var exec = Executors.newFixedThreadPool(3);
        ConcurrentSkipListMap<Digest, Member> serverMembers = new ConcurrentSkipListMap<>();
        serverRouter = new LocalRouter(prefix, serverMembers, builder, exec, null);
        clientRouter = new LocalRouter(prefix, serverMembers, builder, exec, null);

        serverRouter.setMember(serverMember);
        clientRouter.setMember(clientMember);

        serverRouter.start();
        clientRouter.start();

        EventObserver protoService = new EventObserver() {

            @Override
            public CompletableFuture<Void> publish(KERL_ kerl, List<Validations> validations, Digest from) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.complete(null);
                return f;
            }

            @Override
            public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments, Digest from) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.complete(null);
                return f;
            }

            @Override
            public CompletableFuture<Void> publishEvents(List<KeyEvent_> events, List<Validations> validations,
                                                         Digest from) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.complete(null);
                return f;
            }
        };

        ClientIdentity identity = () -> clientMember.getId();
        serverRouter.create(serverMember, context, protoService, protoService.getClass().toString(),
                            r -> new EventObserverServer(r, identity, exec, null), null, null);

        var clientComms = clientRouter.create(clientMember, context, protoService, protoService.getClass().toString(),
                                              r -> new EventObserverServer(r, identity, exec, null),
                                              EventObserverClient.getCreate(context, null), null);

        var client = clientComms.apply(serverMember, clientMember);

        client.publishAttachments(Collections.emptyList()).get();
        client.publish(KERL_.getDefaultInstance(), Collections.emptyList()).get();
        client.publishEvents(Collections.emptyList(), Collections.emptyList()).get();
    }
}
