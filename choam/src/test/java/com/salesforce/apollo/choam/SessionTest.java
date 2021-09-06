/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Outcome;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class SessionTest {
    @Test
    public void func() throws Exception {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 9);
        Parameters params = Parameters.newBuilder().setScheduler(exec).setContext(context)
                                      .setMember(new SigningMemberImpl(Utils.getMember(0))).build();
        @SuppressWarnings("unchecked")
        Function<SubmittedTransaction, ListenableFuture<SubmitResult>> client = stx -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            try {
                stx.onCompletion()
                   .complete(ByteMessage.parseFrom(stx.transaction().getContent()).getContents().toStringUtf8());
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
            SettableFuture<SubmitResult> f = SettableFuture.create();
            f.set(SubmitResult.newBuilder().setOutcome(Outcome.SUCCESS).build());
            return f;
        };
        Session session = new Session(params, client);
        final String content = "Give me food or give me slack or kill me";
        Message tx = ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8(content)).build();
        var result = session.submit(tx, null);
        assertEquals(1, session.submitted());
        assertEquals(content, result.get(1, TimeUnit.SECONDS));
        assertEquals(0, session.submitted());
    }
}
