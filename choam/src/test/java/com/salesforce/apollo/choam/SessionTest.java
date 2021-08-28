/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.google.common.base.Function;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class SessionTest {
    @Test
    public void func() throws Exception {
        Parameters params = Parameters.newBuilder().setMember(new SigningMemberImpl(Utils.getMember(0))).build();
        @SuppressWarnings("unchecked")
        Function<SubmittedTransaction, Boolean> client = stx -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            try {
                stx.onCompletion()
                   .complete(ByteMessage.parseFrom(stx.transaction().getUser()).getContents().toStringUtf8());
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
            return true;
        };
        Session session = Session.newBuilder().build(params, client);
        final String content = "Give me food or give me slack or kill me";
        Message tx = ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8(content)).build();
        var result = session.submit(tx, null);
        assertEquals(1, session.submitted());
        assertEquals(content, result.get(1, TimeUnit.SECONDS));
        assertEquals(0, session.submitted());
    }
}
