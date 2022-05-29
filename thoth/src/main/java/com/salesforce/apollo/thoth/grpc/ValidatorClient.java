/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc;

import java.io.IOException;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.thoth.proto.Signatures;
import com.salesfoce.apollo.thoth.proto.ValidateContext;
import com.salesfoce.apollo.thoth.proto.Validated;
import com.salesfoce.apollo.thoth.proto.ValidatorGrpc;
import com.salesfoce.apollo.thoth.proto.ValidatorGrpc.ValidatorFutureStub;
import com.salesfoce.apollo.thoth.proto.WitnessContext;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.thoth.Sakshi;

/**
 * @author hal.hildebrand
 *
 */
public class ValidatorClient implements ValidatorService {

    public static CreateClientCommunications<ValidatorService> getCreate(Digest context, StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new ValidatorClient(context, c, t, metrics);
        };
    }

    public static ValidatorService getLocalLoopback(Sakshi service, SigningMember member) {
        return new ValidatorService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Validated> validate(List<KeyEventWithAttachments> events) {
                return wrap(service.validate(events));
            }

            @Override
            public ListenableFuture<Signatures> witness(List<KeyEvent_> events, Ident identifier) {
                return wrap(service.witness(events, identifier));
            }
        };
    }

    private static <T> ListenableFuture<T> wrap(T value) {
        SettableFuture<T> fs = SettableFuture.create();
        fs.set(value);
        return fs;
    }

    private final ManagedServerConnection channel;
    private final ValidatorFutureStub     client;
    private final Digest                  context;
    private final Member                  member;
    @SuppressWarnings("unused")
    private final StereotomyMetrics       metrics;

    public ValidatorClient(Digest context, ManagedServerConnection channel, Member member, StereotomyMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = ValidatorGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
        this.context = context;
    }

    @Override
    public void close() throws IOException {
        channel.release();
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Validated> validate(List<KeyEventWithAttachments> events) {
        return client.validate(ValidateContext.newBuilder()
                                              .setContext(context.toDigeste())
                                              .addAllEvents(events)
                                              .build());
    }

    @Override
    public ListenableFuture<Signatures> witness(List<KeyEvent_> events, Ident identifier) {
        return client.witness(WitnessContext.newBuilder()
                                            .setContext(context.toDigeste())
                                            .setIdentifier(identifier)
                                            .addAllEvents(events)
                                            .build());
    }
}
