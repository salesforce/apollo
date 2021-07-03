/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesfoce.apollo.messaging.proto.CausalMessage;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.messaging.causal.CausalBuffer;
import com.salesforce.apollo.membership.messaging.causal.Parameters;
import com.salesforce.apollo.utils.bloomFilters.BloomClock.ClockValueComparator;

/**
 * @author hal.hildebrand
 *
 */
public class CausalBufferTest {

    private SigningMember                              memberA;
    private SigningMember                              memberB;
    private Context<Member>                            context;
    private Cuckoo                                     clock;
    private Parameters.Builder                         parameters;
    private List<Map<Digest, List<CausalMessage>>>     aDelivered;
    private List<Map<Digest, List<CausalMessage>>>     bDelivered;
    private Any                                        content;
    private List<CausalMessage>                        aSends;
    private List<CausalMessage>                        bSends;
    private Consumer<Map<Digest, List<CausalMessage>>> aDelivery;
    private Consumer<Map<Digest, List<CausalMessage>>> bDelivery;
    private CausalBuffer                               bufferA;
    private CausalBuffer                               bufferB;

    private static class Cuckoo extends Clock {
        long instant = 0;

        @Override
        public ZoneId getZone() {
            return ZoneId.systemDefault();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return null;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(instant);
        }

    }

    @BeforeEach
    public void setup() {
        memberA = new SigningMemberImpl(getMember(0));
        memberB = new SigningMemberImpl(getMember(1));
        context = new Context<Member>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1));
        context.activate(memberA);
        context.activate(memberB);

        clock = new Cuckoo();

        parameters = Parameters.newBuilder()
                               .setComparator(new ClockValueComparator(0.01))
                               .setWallclock(clock)
                               .setContext(context);
        aDelivered = new ArrayList<>();
        bDelivered = new ArrayList<>();
        aSends = new ArrayList<>();
        bSends = new ArrayList<>();
        aDelivery = mail -> aDelivered.add(mail);
        bDelivery = mail -> bDelivered.add(mail);
        bufferA = new CausalBuffer(parameters.setMember(memberA).build(), aDelivery);
        bufferB = new CausalBuffer(parameters.setMember(memberB).build(), bDelivery);
        content = Any.pack(ByteMessage.getDefaultInstance());
    }

    @Test
    public void smokeIt() {
        CausalMessage aEvent, bEvent;
        aEvent = sendA();

        assertNotNull(aEvent);
        assertEquals(memberA.getId(), new Digest(aEvent.getSource()));
        assertEquals(0, aDelivered.size());

        bEvent = sendB();

        assertNotNull(bEvent);
        assertEquals(memberB.getId(), new Digest(bEvent.getSource()));
        assertEquals(0, aDelivered.size());

        deliverA(bEvent);
        assertEquals(1, aDelivered.size());

        deliverA(bEvent);
        assertEquals(1, aDelivered.size());

        deliverB(aEvent);
        assertEquals(1, bDelivered.size());

        deliverB(aEvent);
        assertEquals(1, bDelivered.size());

        int sends = 1_000;
        for (int i = 0; i < sends; i++) {
            aEvent = sendA();
            deliverB(aEvent);

            bEvent = sendB();
            deliverA(bEvent);

            boolean pass = i + 2 == aDelivered.size();
            if (!pass) {
                deliverA(bEvent);
            }
            assertEquals(i + 2, aDelivered.size());
            boolean pass2 = i + 2 == bDelivered.size();
            if (!pass2) {
                deliverB(aEvent);
            }
            assertEquals(i + 2, bDelivered.size());
        }
        assertEquals(sends + 1, aDelivered.size());
        assertEquals(sends + 1, bDelivered.size());
    }

    private void deliverB(CausalMessage aEvent) {
        bufferB.deliver(Arrays.asList(aEvent));
    }

    private void deliverA(CausalMessage bEvent) {
        bufferA.deliver(Arrays.asList(bEvent));
    }

    private CausalMessage sendB() {
        clock.instant += 1;
        CausalMessage bEvent;
        bEvent = bufferB.send(content, memberB);
        bSends.add(bEvent);
        clock.instant += 1;
        return bEvent;
    }

    private CausalMessage sendA() {
        clock.instant += 1;
        CausalMessage aEvent;
        aEvent = bufferA.send(content, memberA);
        aSends.add(aEvent);
        clock.instant += 1;
        return aEvent;
    }
}
