/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications.netty;

import static com.salesforce.apollo.comm.netty4.MtlsServer.defaultBuiilder;

import java.security.cert.X509Certificate;
import java.util.function.Function;

import javax.net.ssl.SSLException;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;

import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.comm.netty4.MtlsServer;
import com.salesforce.apollo.comm.netty4.NettyTlsTransceiver;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.communications.netty.CommonNettyCommunications;
import com.salesforce.apollo.ghost.Ghost;
import com.salesforce.apollo.ghost.Ghost.Service;
import com.salesforce.apollo.ghost.communications.GhostClientCommunications;
import com.salesforce.apollo.ghost.communications.GhostCommunications;
import com.salesforce.apollo.ghost.communications.GhostServerCommunications;

import io.netty.handler.ssl.ClientAuth;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostNettyCommunications extends CommonNettyCommunications implements GhostCommunications {
	private volatile Ghost ghost;

	@Override
	public GhostClientCommunications connect(Member to, Node from) {
		try {
			GhostClientCommunications thisOutbound[] = new GhostClientCommunications[1];
			GhostClientCommunications outbound = new GhostClientCommunications(
					new NettyTlsTransceiver(to.getGhostEndpoint(), forClient(from).build(), eventGroup) {

						@Override
						public void close() {
							openOutbound.remove(thisOutbound[0]);
							try {
								super.close();
							} catch (Throwable e) {
								log.info("error closing connection to " + to, e);
							}
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
	public void initialize(Ghost ghost) {
		this.ghost = ghost;
	}

	@Override
	protected MtlsServer newServer() {
		try {
			return new MtlsServer(ghost.getNode().getGhostEndpoint(),
					forServer(ghost.getNode()).clientAuth(ClientAuth.REQUIRE).build(), provider(), defaultBuiilder(), 1,
					"Ghost[" + ghost.getNode().getId() + "]", ghost.getNode().getParameters().rings);
		} catch (SSLException e) {
			throw new IllegalStateException("Unable to construct SslContext for " + ghost.getNode().getId(), e);
		}
	}

	private Function<X509Certificate, Responder> provider() {
		return certificate -> {
			Service service = ghost.getService();
			SpecificResponder responder = new SpecificResponder(Apollo.class, new GhostServerCommunications(service));
			return responder;
		};
	}

}
