/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.client.client.comm;

import java.io.IOException;
import java.time.Duration;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Admissions extends Link {

    static Admissions getLocalLoopback(Member node) {
        return new Admissions() {

            @Override
            public ListenableFuture<SignedNonce> apply(KERL_ application, Duration timeout) {
                return null;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return node;
            }

            @Override
            public ListenableFuture<Validations> register(Credentials credentials, Duration timeout) {
                return null;
            }
        };
    }

    ListenableFuture<SignedNonce> apply(KERL_ application, Duration timeout);

    ListenableFuture<Validations> register(Credentials credentials, Duration timeout);
}
