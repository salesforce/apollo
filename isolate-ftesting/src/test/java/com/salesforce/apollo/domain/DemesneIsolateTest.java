/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.domain;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getServerDomainSocketChannelClass;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.Demesne;
import com.salesforce.apollo.model.DemesneImpl;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLServer;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import com.salesforce.apollo.utils.Utils;

import io.grpc.ManagedChannel;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.ServerDomainSocketChannel;

/**
 * @author hal.hildebrand
 *
 */
public class DemesneIsolateTest {
    private static final Class<? extends ServerDomainSocketChannel> channelType    = getServerDomainSocketChannelClass();
    private EventLoopGroup                                          eventLoopGroup = getEventLoopGroup();

    @Test
    public void smokin() throws Exception {
        var commDirectory = Path.of("target").resolve(UUID.randomUUID().toString());
        Files.createDirectories(commDirectory);
        final var ksPassword = new char[] { 'f', 'o', 'o' };
        final var ks = KeyStore.getInstance("JKS");
        ks.load(null, ksPassword);
        final var keystore = new JksKeyStore(ks, () -> ksPassword);
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        Stereotomy controller = new StereotomyImpl(keystore, kerl, SecureRandom.getInstanceStrong());
        var identifier = controller.newIdentifier().get();
        var baos = new ByteArrayOutputStream();
        ks.store(baos, ksPassword);
        ProtoKERLService protoService = new ProtoKERLAdapter(kerl);
        Member serverMember = new ControlledIdentifierMember(identifier);
        var kerlEndpoint = UUID.randomUUID().toString();
        final var portalEndpoint = new DomainSocketAddress(commDirectory.resolve(kerlEndpoint).toFile());
        var serverBuilder = NettyServerBuilder.forAddress(portalEndpoint)
                                              .protocolNegotiator(new DomainSocketNegotiator())
                                              .channelType(channelType)
                                              .workerEventLoopGroup(eventLoopGroup)
                                              .bossEventLoopGroup(eventLoopGroup)
                                              .intercept(new DomainSocketServerInterceptor());

        var cacheBuilder = ServerConnectionCache.newBuilder().setFactory(to -> handler(portalEndpoint));
        var router = new Router(serverMember, serverBuilder, cacheBuilder, null);
        router.start();
        Digest context = DigestAlgorithm.DEFAULT.getOrigin();
        @SuppressWarnings("unused")
        var comms = router.create(serverMember, context, protoService, protoService.getClass().getCanonicalName(),
                                  r -> new KERLServer(r, null), null, null);

        var parameters = DemesneParameters.newBuilder()
                                          .setKerlContext(context.toDigeste())
                                          .setKerlService(kerlEndpoint)
                                          .setMember(identifier.getIdentifier().toIdent())
                                          .setKeyStore(ByteString.copyFrom(baos.toByteArray()))
                                          .setCommDirectory(commDirectory.toString())
                                          .build();
        Demesne demesne = new DemesneImpl(parameters, ksPassword);
        demesne.start();
        Utils.waitForCondition(1000, () -> demesne.active());
    }

    private ManagedChannel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .eventLoopGroup(eventLoopGroup)
                                  .channelType(channelType)
                                  .keepAliveTime(1, TimeUnit.SECONDS)
                                  .usePlaintext()
                                  .build();
    }
}
