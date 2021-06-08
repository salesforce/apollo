/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import static io.github.olivierlemasle.ca.CA.createCsr;
import static io.github.olivierlemasle.ca.CA.dn;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.InetSocketAddress;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.proto.AvalancheGrpc;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.SuppliedDagNodes;
import com.salesforce.apollo.crypto.cert.CaValidator;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.fireflies.ca.CertificateAuthority;
import com.salesforce.apollo.utils.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.CsrWithPrivateKey;
import io.github.olivierlemasle.ca.RootCertificate;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class TestMtls {

    private CertificateAuthority ca;

    @Test
    public void smoke() throws Exception {
        ca = certAuth();

        InetSocketAddress serverAddress = new InetSocketAddress("localhost", Utils.allocatePort());

        MtlsServer server = server(ca, serverAddress);
        server.start();
        server.bind(avaServer());
        MtlsClient client = client(ca, serverAddress);

        QueryResult query = AvalancheGrpc.newBlockingStub(client.getChannel()).query(null);

        assertNotNull(query);
    }

    private AvalancheImplBase avaServer() {
        return new AvalancheImplBase() {

            @Override
            public void query(Query request, StreamObserver<QueryResult> responseObserver) {
                responseObserver.onNext(QueryResult.newBuilder().build());
                responseObserver.onCompleted();
            }

            @Override
            public void requestDag(DagNodes request, StreamObserver<SuppliedDagNodes> responseObserver) {
                responseObserver.onNext(SuppliedDagNodes.newBuilder().build());
                responseObserver.onCompleted();
            }
        };
    }

    private CertificateAuthority certAuth() {
        RootCertificate root = CertificateAuthority.mint(dn().setCn("test-ca.com")
                                                             .setO("World Company")
                                                             .setOu("IT dep")
                                                             .setSt("CA")
                                                             .setC("US")
                                                             .build(),
                                                         12, .1, .1, null);
        CertificateAuthority ca = new CertificateAuthority(root);
        return ca;
    }

    private MtlsClient client(CertificateAuthority ca, InetSocketAddress serverAddress) {
        CertificateWithPrivateKey clientCert = clientIdentity(ca);

        MtlsClient client = new MtlsClient(serverAddress, ClientAuth.REQUIRE, "foo", clientCert.getX509Certificate(),
                clientCert.getPrivateKey(), validator());
        return client;
    }

    private CertificateWithPrivateKey clientIdentity(CertificateAuthority ca) {
        CsrWithPrivateKey clientRequest = createCsr().generateRequest(dn().setCn("localhost")
                                                                          .setO("World Company")
                                                                          .setOu("IT dep")
                                                                          .setSt("CA")
                                                                          .setC("US")
                                                                          .build());
        CertificateWithPrivateKey clientCert = ca.mintNode(clientRequest)
                                                 .attachPrivateKey(clientRequest.getPrivateKey());
        return clientCert;
    }

    private MtlsServer server(CertificateAuthority ca, InetSocketAddress serverAddress) {
        CertificateWithPrivateKey serverCert = serverIdentity(ca);

        MtlsServer server = new MtlsServer(serverAddress, ClientAuth.REQUIRE, "foo", serverCert.getX509Certificate(),
                serverCert.getPrivateKey(), validator(), new MutableHandlerRegistry(), ForkJoinPool.commonPool());
        return server;
    }

    private CertificateWithPrivateKey serverIdentity(CertificateAuthority ca) {
        CsrWithPrivateKey serverRequest = createCsr().generateRequest(dn().setCn("localhost")
                                                                          .setO("World Company")
                                                                          .setOu("IT dep")
                                                                          .setSt("CA")
                                                                          .setC("US")
                                                                          .build());

        CertificateWithPrivateKey serverCert = ca.mintNode(serverRequest)
                                                 .attachPrivateKey(serverRequest.getPrivateKey());
        return serverCert;
    }

    private CertificateValidator validator() {
        return new CaValidator(ca.getRoot());
    }
}
