/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.SecureRandom;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.resolver.ResolverClient;
import com.salesforce.apollo.stereotomy.services.grpc.resolver.ResolverServer;
import com.salesforce.apollo.stereotomy.services.proto.ProtoResolver;

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

        ProtoResolver protoService = new ProtoResolver() {

            @Override
            public Optional<Binding> lookup(Ident prefix) {
                return Optional.of(Binding.getDefaultInstance());
            }
        };

        serverRouter.create(serverMember, context, protoService, protoService.getClass().toString(),
                            r -> new ResolverServer(r, exec, null), null, null);

        var clientComms = clientRouter.create(clientMember, context, protoService, protoService.getClass().toString(),
                                              r -> new ResolverServer(r, exec, null),
                                              ResolverClient.getCreate(context, null), null);

        var client = clientComms.apply(serverMember, clientMember);

        assertEquals(Binding.getDefaultInstance(), client.lookup(Ident.getDefaultInstance()).get());
    }
}
