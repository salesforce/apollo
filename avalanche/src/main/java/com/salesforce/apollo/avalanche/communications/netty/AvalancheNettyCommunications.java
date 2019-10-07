/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications.netty;

import static com.salesforce.apollo.comm.netty4.MtlsServer.defaultBuiilder;

import java.security.cert.X509Certificate;
import java.util.function.Function;

import javax.net.ssl.SSLException;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;

import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.Avalanche.Service;
import com.salesforce.apollo.avalanche.communications.AvalancheClientCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheServerCommunications;
import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.comm.netty4.MtlsServer;
import com.salesforce.apollo.comm.netty4.NettyTlsTransceiver;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.communications.netty.CommonNettyCommunications;

import io.netty.handler.ssl.ClientAuth;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheNettyCommunications extends CommonNettyCommunications implements AvalancheCommunications {
	private volatile Avalanche avalanche;

	@Override
	public AvalancheClientCommunications connectToNode(Member to, Node from) {
		try {
			AvalancheClientCommunications thisOutbound[] = new AvalancheClientCommunications[1];
			AvalancheClientCommunications outbound = new AvalancheClientCommunications(
					new NettyTlsTransceiver(to.getAvalancheEndpoint(), forClient(from).build(), eventGroup) {

						@Override
						public void close() {
							openOutbound.remove(thisOutbound[0]);
							super.close();
						}

					}, to);
			thisOutbound[0] = outbound;
			openOutbound.add(outbound);
			return outbound;
		} catch (Throwable e) {
			log.debug("Error connecting to {}", to, e);
			return null;
		}
	}

	@Override
	public void initialize(Avalanche avalanche) {
		this.avalanche = avalanche;
	}

	@Override
	protected MtlsServer newServer() {
		try {
			return new MtlsServer(avalanche.getNode().getAvalancheEndpoint(),
					forServer(avalanche.getNode()).clientAuth(ClientAuth.NONE).build(), provider(), defaultBuiilder(),
					1, "Ava[" + avalanche.getNode().getId() + "]", avalanche.getNode().getParameters().rings);
		} catch (SSLException e) {
			throw new IllegalStateException("Unable to construct SslContext for " + avalanche.getNode().getId());
		}
	}

	private Function<X509Certificate, Responder> provider() {
		return certificate -> {
			Service service = avalanche.getService();
			SpecificResponder responder = new SpecificResponder(Apollo.class,
					new AvalancheServerCommunications(service));
			return responder;
		};
	}

}
