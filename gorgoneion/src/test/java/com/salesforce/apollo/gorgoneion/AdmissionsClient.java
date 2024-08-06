/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.gorgoneion.proto.AdmissionsGrpc;
import com.salesforce.apollo.gorgoneion.proto.Credentials;
import com.salesforce.apollo.gorgoneion.proto.Establishment;
import com.salesforce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.event.proto.KERL_;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author hal.hildebrand
 */
public class AdmissionsClient implements Admissions {

    private final ManagedServerChannel                  channel;
    private final AdmissionsGrpc.AdmissionsBlockingStub client;

    public AdmissionsClient(ManagedServerChannel channel) {
        this.channel = channel;
        this.client = channel.wrap(AdmissionsGrpc.newBlockingStub(channel));
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
    public Establishment register(Credentials credentials, Duration timeout) {
        return client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS).register(credentials);
    }
}
