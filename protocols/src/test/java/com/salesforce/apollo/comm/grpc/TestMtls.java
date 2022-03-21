/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Provider;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItImplBase;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.utils.Utils;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class TestMtls {

    @Test
    public void smoke() throws Exception {
        InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getLocalHost(), Utils.allocatePort());

        MtlsServer server = server(serverAddress);
        try {
            server.start();
            server.bind(server());
            Thread.sleep(1_000);
            MtlsClient client = client(serverAddress);

            for (int i = 0; i < 100; i++) {
                Any tst = TestItGrpc.newBlockingStub(client.getChannel()).ping(Any.getDefaultInstance());

                assertNotNull(tst);
            }
        } finally {
            server.stop();
        }
    }

    private TestItImplBase server() {
        return new TestItImplBase() {

            @Override
            public void ping(Any request, StreamObserver<Any> responseObserver) {
                responseObserver.onNext(Any.newBuilder().build());
                responseObserver.onCompleted();
            }
        };
    }

    private MtlsClient client(InetSocketAddress serverAddress) {
        CertificateWithPrivateKey clientCert = clientIdentity();

        MtlsClient client = new MtlsClient(serverAddress, ClientAuth.REQUIRE, "foo", clientCert.getX509Certificate(),
                                           clientCert.getPrivateKey(), validator());
        return client;
    }

    private CertificateWithPrivateKey clientIdentity() {
        return Utils.getMember(0);
    }

    private MtlsServer server(InetSocketAddress serverAddress) {
        CertificateWithPrivateKey serverCert = serverIdentity();

        MtlsServer server = new MtlsServer(serverAddress, ClientAuth.REQUIRE, "foo", new ServerContextSupplier() {

            @Override
            public Digest getMemberId(X509Certificate key) {
                return Digest.NONE;
            }

            @Override
            public SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator,
                                        Provider provider, String tlsVersion) {
                return MtlsServer.forServer(clientAuth, alias, serverCert.getX509Certificate(),
                                            serverCert.getPrivateKey(), validator);
            }
        }, validator(), new MutableHandlerRegistry(), ForkJoinPool.commonPool());
        return server;
    }

    private CertificateWithPrivateKey serverIdentity() {
        return Utils.getMember(1);
    }

    private CertificateValidator validator() {
        return new CertificateValidator() {
            @Override
            public void validateServer(X509Certificate[] chain) throws CertificateException {
            }

            @Override
            public void validateClient(X509Certificate[] chain) throws CertificateException {
            }
        };
    }
}
