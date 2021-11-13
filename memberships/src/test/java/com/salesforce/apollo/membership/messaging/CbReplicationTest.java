/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.membership.messaging.causal.CausalBuffer.hashOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.messaging.causal.CausalBuffer;
import com.salesforce.apollo.membership.messaging.causal.CausalBuffer.StampedMessage;
import com.salesforce.apollo.membership.messaging.causal.Parameters;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CbReplicationTest {

    private SigningMember                           memberA;
    private SigningMember                           memberB;
    private Context<Member>                         context;
    private Parameters.Builder                      parameters;
    private List<Map<Digest, List<StampedMessage>>> aDelivered;
    private List<Map<Digest, List<StampedMessage>>> bDelivered;
    private Any                                     content;
    private List<StampedMessage>                    aSends;
    private List<StampedMessage>                    bSends;
    private CausalBuffer                            bufferA;
    private CausalBuffer                            bufferB;

    @BeforeEach
    public void setup() {
        memberA = new SigningMemberImpl(Utils.getMember(0));
        memberB = new SigningMemberImpl(Utils.getMember(1));
        context = new Context<Member>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1));
        context.activate(memberA);
        context.activate(memberB);

        parameters = Parameters.newBuilder().setClockK(2).setClockM(512).setClockK(4).setBufferSize(4000)
                               .setFalsePositiveRate(0.0125).setExecutor(ForkJoinPool.commonPool()).setContext(context);
        aDelivered = new ArrayList<>();
        bDelivered = new ArrayList<>();
        aSends = new ArrayList<>();
        bSends = new ArrayList<>();
        Consumer<Map<Digest, List<StampedMessage>>> aDelivery = mail -> {
            aDelivered.add(mail);
        };
        Consumer<Map<Digest, List<StampedMessage>>> bDelivery = mail -> {
            bDelivered.add(mail);
        };
        bufferA = new CausalBuffer(parameters.setMember(memberA).build(), aDelivery);
        bufferB = new CausalBuffer(parameters.setMember(memberB).build(), bDelivery);
        content = Any.pack(ByteMessage.getDefaultInstance());
    }

//    @Test TODO deprecate
    public void idealReplication() {
        var sentA = new HashMap<>();
        var sentB = new HashMap<>();

        Set<Digest> receivedA = new HashSet<>();
        Set<Digest> receivedB = new HashSet<>();

        for (int i = 0; i < 1000; i++) {
            StampedMessage a = sendA();
            sentA.put(a.hash(), a);
            StampedMessage b = sendB();
            sentB.put(b.hash(), b);
        }

        for (int i = 0; i < 1000 / parameters.getMaxMessages(); i++) {
            var reconA = bufferA.forReconcilliation();
            var reconB = bufferB.forReconcilliation();

            var reconciled = bufferA.reconcile(reconB, memberB.getId());
            assertEquals(parameters.getMaxMessages(), reconciled.size(), "Failed on: " + i);
            receivedB.addAll(reconciled.stream().map(e -> hashOf(e, parameters.getDigestAlgorithm())).toList());
            bufferB.receive(reconciled);

            reconciled = bufferB.reconcile(reconA, memberA.getId());
            assertEquals(parameters.getMaxMessages(), reconciled.size(), "Failed on: " + i);
            receivedA.addAll(reconciled.stream().map(e -> hashOf(e, parameters.getDigestAlgorithm())).toList());
            bufferA.receive(reconciled);

            int expected = (i + 1) * parameters.getMaxMessages();
            assertEquals(expected, receivedA.size(), "Failed on: " + i);
            assertEquals(expected, receivedB.size(), "Failed on: " + i);
            assertEquals(i + 1, aDelivered.size(), "Failed on: " + i);
            assertEquals(i + 1, bDelivered.size(), "Failed on: " + i);
        }

        assertEquals(1000, aDelivered.stream().flatMap(m -> m.values().stream()).flatMap(l -> l.stream())
                                     .collect(Collectors.toSet()).size());
        assertEquals(1000, bDelivered.stream().flatMap(m -> m.values().stream()).flatMap(l -> l.stream())
                     .collect(Collectors.toSet()).size());

        for (int i = 0; i < 1000; i++) {
            var reconA = bufferA.forReconcilliation();
            var reconB = bufferB.forReconcilliation();

            var reconciled = bufferA.reconcile(reconB, memberB.getId());
            assertEquals(0, reconciled.size(), "Failed on: " + i);
            reconciled = bufferB.reconcile(reconA, memberA.getId());
            assertEquals(0, reconciled.size(), "Failed on: " + i);
        }
    }

    @SuppressWarnings("unused")
    private void deliverB(StampedMessage aEvent) {
        bufferB.receive(Arrays.asList(aEvent.message().build()));
    }

    @SuppressWarnings("unused")
    private void deliverA(StampedMessage bEvent) {
        bufferA.receive(Arrays.asList(bEvent.message().build()));
    }

    private StampedMessage sendB() {
        StampedMessage bEvent;
        bEvent = bufferB.send(content, memberB);
        bSends.add(bEvent);
        return bEvent;
    }

    private StampedMessage sendA() {
        StampedMessage aEvent;
        aEvent = bufferA.send(content, memberA);
        aSends.add(aEvent);
        return aEvent;
    }
}
