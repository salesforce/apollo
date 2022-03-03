/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.services.impl.ProtoKERLService;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestResolver {

    private LocalRouter serverRouter;
    private LocalRouter clientRouter;

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
    public void smokin() throws Exception {
        var context = DigestAlgorithm.DEFAULT.getLast().prefix("foo");
        var prefix = UUID.randomUUID().toString();

        var serverMember = new SigningMemberImpl(Utils.getMember(0));
        var clientMember = new SigningMemberImpl(Utils.getMember(1));

        var builder = ServerConnectionCache.newBuilder();
        serverRouter = new LocalRouter(prefix, serverMember, builder, ForkJoinPool.commonPool());
        clientRouter = new LocalRouter(prefix, clientMember, builder, ForkJoinPool.commonPool());

        serverRouter.start();
        clientRouter.start();

        KERL kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        ProtoKERLService protoService = new ProtoKERLService(kerl);

        @SuppressWarnings("unused")
        var serverComms = serverRouter.create(serverMember, context, protoService, r -> new KERLServer(null, r), null,
                                              null);

        var clientComms = clientRouter.create(clientMember, context, protoService, r -> new KERLServer(null, r),
                                              KERLClient.getCreate(context, null), null);

        var client = clientComms.apply(serverMember, clientMember);

        client.resolve(new SelfAddressingIdentifier(context));
    }
}
