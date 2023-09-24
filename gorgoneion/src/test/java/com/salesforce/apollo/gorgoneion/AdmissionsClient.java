/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import com.salesfoce.apollo.gorgoneion.proto.AdmissionsGrpc;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author hal.hildebrand
 */
public class AdmissionsClient implements Admissions {

    private final ManagedServerChannel channel;
    private final AdmissionsGrpc.AdmissionsBlockingStub client;
    public AdmissionsClient(ManagedServerChannel channel) {
        this.channel = channel;
        this.client = AdmissionsGrpc.newBlockingStub(channel).withCompression("gzip");
    }

    public static CreateClientCommunications<Admissions> getCreate() {
        return (c) -> new AdmissionsClient(c);

    }

    @Override
    public SignedNonce apply(KERL_ application, Duration timeout) {
        return client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS).apply(application);
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public Validations register(Credentials credentials, Duration timeout) {
        return client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS).register(credentials);
    }
}
