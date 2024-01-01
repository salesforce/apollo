/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.kerl;

import com.salesforce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;

/**
 * @author hal.hildebrand
 */
public class KERLClient extends CommonKERLClient implements KERLService {

    private final ManagedServerChannel channel;

    public KERLClient(ManagedServerChannel channel, StereotomyMetrics metrics) {
        super(KERLServiceGrpc.newBlockingStub(channel).withCompression("gzip"), metrics);
        this.channel = channel;
    }

    public static CreateClientCommunications<KERLService> getCreate(StereotomyMetrics metrics) {
        return (c) -> {
            return new KERLClient(c, metrics);
        };

    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }
}
