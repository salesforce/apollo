/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.admissions;

import java.io.IOException;
import java.time.Duration;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Admissions extends Link {

    static Admissions getLocalLoopback(Member node) {
        return new Admissions() {

            @Override
            public ListenableFuture<SignedNonce> apply(Application application, Duration timeout) {
                return null;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<Empty> enroll(Notarization notarization, Duration timeout) {
                return null;
            }

            @Override
            public Member getMember() {
                return node;
            }

            @Override
            public ListenableFuture<Invitation> register(Credentials credentials, Duration timeout) {
                return null;
            }
        };
    }

    ListenableFuture<SignedNonce> apply(Application application, Duration timeout);

    ListenableFuture<Empty> enroll(Notarization notarization, Duration timeout);

    ListenableFuture<Invitation> register(Credentials credentials, Duration timeout);
}
