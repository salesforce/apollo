/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.proto.AvalancheGrpc;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.SuppliedDagNodes;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.cert.BcX500NameDnImpl;
import com.salesforce.apollo.crypto.cert.CertExtension;
import com.salesforce.apollo.crypto.cert.Certificates;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.utils.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.CertificateWithPrivateKeyImpl;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class TestMtlsStereotomy {
    static {
        ProviderUtils.setup(true, true, false);
    }

    @Test
    public void smoke() throws Exception {
        InetSocketAddress serverAddress = new InetSocketAddress("localhost", Utils.allocatePort());

        MtlsServer server = server(serverAddress);
        server.start();
        server.bind(avaServer());
        MtlsClient client = client(serverAddress);

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

    private MtlsClient client(InetSocketAddress serverAddress) {
        CertificateWithPrivateKey clientCert = clientIdentity();

        MtlsClient client = new MtlsClient(serverAddress, ClientAuth.REQUIRE, "foo", clientCert.getX509Certificate(),
                clientCert.getPrivateKey(), validator());
        return client;
    }

    private CertificateWithPrivateKey clientIdentity() {
        return generate(new BcX500NameDnImpl("CN=0fgdSAGdx_"));
    }

    CertificateWithPrivateKey generate(BcX500NameDnImpl dn) {
        BigInteger sn = BigInteger.valueOf(Long.MAX_VALUE);
        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plusSeconds(10_000));
        List<CertExtension> extensions = Collections.emptyList();
        KeyPair keyPair = SignatureAlgorithm.ED_25519.generateKeyPair();
        X509Certificate selfSignedCert = Certificates.selfSign(true, dn, sn, keyPair, notBefore, notAfter, extensions);
        return new CertificateWithPrivateKeyImpl(selfSignedCert, keyPair.getPrivate());
    }

    private MtlsServer server(InetSocketAddress serverAddress) {
        CertificateWithPrivateKey serverCert = serverIdentity();

        MtlsServer server = new MtlsServer(serverAddress, ClientAuth.REQUIRE, "foo", serverCert.getX509Certificate(),
                serverCert.getPrivateKey(), validator(), new MutableHandlerRegistry(), ForkJoinPool.commonPool());
        return server;
    }

    private CertificateWithPrivateKey serverIdentity() {
        return generate(new BcX500NameDnImpl("CN=0fgadasdf3Q@SAGdx_"));
    }

    private CertificateValidator validator() {
        return new CertificateValidator() {

            @Override
            public void validateServer(X509Certificate[] chain) throws CertificateException {
                // TODO Auto-generated method stub

            }

            @Override
            public void validateClient(X509Certificate[] chain) throws CertificateException {
                // TODO Auto-generated method stub

            }
        };
    }
}
