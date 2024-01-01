/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 */
public class TerminalClient implements Terminal {

    private final ManagedServerChannel              channel;
    private final TerminalGrpc.TerminalBlockingStub client;
    @SuppressWarnings("unused")
    private final ChoamMetrics                      metrics;

    public TerminalClient(ManagedServerChannel channel, ChoamMetrics metrics) {
        this.channel = channel;
        this.client = TerminalGrpc.newBlockingStub(channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public static CreateClientCommunications<Terminal> getCreate(ChoamMetrics metrics) {
        return (c) -> new TerminalClient(c, metrics);

    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public CheckpointSegments fetch(CheckpointReplication request) {
        return client.fetch(request);
    }

    @Override
    public Blocks fetchBlocks(BlockReplication replication) {
        return client.fetchBlocks(replication);
    }

    @Override
    public Blocks fetchViewChain(BlockReplication replication) {
        return client.fetchViewChain(replication);
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public ViewMember join(Digest nextView) {
        return client.join(nextView.toDigeste());
    }

    public void release() {
        close();
    }

    @Override
    public Initial sync(Synchronize sync) {
        return client.sync(sync);
    }
}
