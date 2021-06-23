/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 *
 */
public interface LinearService extends Link {

    static LinearService getLocalLoopback(SigningMember member) {
        return new LinearService() {

            @Override
            public ListenableFuture<TransactionResult> clientSubmit(SubmitTransaction request) {
                return null;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<JoinResult> join(Join join) {
                return null;
            }

            @Override
            public void stopData(StopData stopData) {
            }
        };
    }

    ListenableFuture<TransactionResult> clientSubmit(SubmitTransaction request);

    ListenableFuture<JoinResult> join(Join join);

    void stopData(StopData stopData);
}
