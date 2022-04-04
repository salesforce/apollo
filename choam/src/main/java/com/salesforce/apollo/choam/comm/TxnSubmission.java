/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import java.io.IOException;

import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 *
 */
public interface TxnSubmission extends Link {
    static TxnSubmission getLocalLoopback(SigningMember member, Submitter service) {
        return new TxnSubmission() {

            @Override
            public void close() throws IOException {

            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public SubmitResult submit(SubmitTransaction request) {
                return service.submit(request, member.getId());
            }
        };
    }

    SubmitResult submit(SubmitTransaction request);
}
