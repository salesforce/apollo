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
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.proto.AvalancheGrpc;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesforce.apollo.fireflies.ca.CertificateAuthority;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.CsrWithPrivateKey;
import io.github.olivierlemasle.ca.RootCertificate;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class TestMtls {

    @Test
    public void smoke() throws Exception {
        CertificateAuthority ca = certAuth();

        InetSocketAddress serverAddress = new InetSocketAddress("localhost", Utils.allocatePort());

        MtlsServer server = server(ca, serverAddress);
        server.start();

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
            public void requestDag(DagNodes request, StreamObserver<DagNodes> responseObserver) {
                responseObserver.onNext(DagNodes.newBuilder().build());
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
                clientCert.getPrivateKey(), ca.getRoot());
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

        MtlsServer server = new MtlsServer(Arrays.asList(avaServer()), serverAddress, ClientAuth.REQUIRE, "foo",
                serverCert.getX509Certificate(), serverCert.getPrivateKey(), ca.getRoot());
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

}
