/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.Interval;
import com.salesforce.apollo.comm.CommonClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.SpaceGhost;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostClientCommunications extends CommonClientCommunications implements SpaceGhost {
	private static final Logger log = LoggerFactory.getLogger(GhostClientCommunications.class);
	private final Apollo client;
	private final Transceiver transceiver;

	public GhostClientCommunications(Transceiver transceiver, Member member)   {
		super(member);
		this.transceiver = transceiver;
		try {
			client = SpecificRequestor.getClient(Apollo.class, transceiver);
		} catch (IOException e) {
			throw new AvroRemoteException("Cannot create proxy rpc client to: " + member + " : " + transceiver, e);
		}
	}

	@Override
	public void close() {
		try {
			transceiver.close();
		} catch (IOException e) {
			log.trace("error closing communications with " + member, e);
		}
	}

	@Override
	public DagEntry get(HashKey DagEntry)   {
		return client.get(DagEntry);
	}

	@Override
	public List<DagEntry> intervals(List<Interval> intervals, List<HashKey> have) {
		return client.intervals(intervals, have);
	}

	@Override
	public void put(DagEntry value) {
		client.put(value);
	}

}
