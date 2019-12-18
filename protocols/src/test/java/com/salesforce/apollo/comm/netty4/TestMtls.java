/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.comm.netty4;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.specific.SpecificData;
import org.junit.Test;

import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.Digests;
import com.salesforce.apollo.avro.Gossip;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.Interval;
import com.salesforce.apollo.avro.QueryResult;
import com.salesforce.apollo.avro.Signed;
import com.salesforce.apollo.avro.Update;
import com.salesforce.apollo.protocols.Utils;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

/**
 * @author hhildebrand
 */
public class TestMtls {

    @Test
    public void testIt() throws Exception {
        InetSocketAddress serverAddress = new InetSocketAddress("localhost", Utils.allocatePort());

        SelfSignedCertificate ssc = new SelfSignedCertificate();
        Function<X509Certificate, Responder> responderProvider = certificate -> new SpecificResponder(Apollo.class,
                service());
        MtlsServer server = new MtlsServer(serverAddress,
                SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build(), responderProvider,
                MtlsServer.defaultBuiilder(), new NioEventLoopGroup(), new NioEventLoopGroup(),
                new DefaultEventExecutorGroup(1));
        NettyTlsTransceiver transceiver = new NettyTlsTransceiver(serverAddress,
                SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build(),
                new NioEventLoopGroup(), new DefaultEventExecutorGroup(1));

        SpecificRequestor requestor = new SpecificRequestor(Apollo.PROTOCOL, transceiver, SpecificData.get());
        Apollo client = SpecificRequestor.getClient(Apollo.class, requestor);
        for (int i = 0; i < 10000; i++)
            assertEquals(110, client.ping(0));

        transceiver.close();
        server.close();
    }

    private Apollo service() {
        return new Apollo() {

            @Override
            public DagEntry get(HASH key) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Gossip gossip(Signed note, int ring, Digests gossip) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public List<DagEntry> intervals(List<Interval> intervals, List<HASH> have) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public int ping(int ping) {
                // TODO Auto-generated method stub
                return 110;
            }

            @Override
            public void put(DagEntry DagEntry) {
                // TODO Auto-generated method stub

            }

            @Override
            public QueryResult query(List<ByteBuffer> transactions, List<HASH> wanted) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public List<ByteBuffer> requestDag(List<HASH> wanted) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void update(int ring, Update update) {
                // TODO Auto-generated method stub

            }
        };
    }

}
