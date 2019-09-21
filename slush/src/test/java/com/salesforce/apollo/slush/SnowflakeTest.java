/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.slush;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.slush.Color;
import com.salesforce.apollo.slush.Snowball;
import com.salesforce.apollo.slush.config.SnowflakeParameters;

/**
 * @author hal.hildebrand
 * @since 220
 */
@SuppressWarnings("rawtypes")
public class SnowflakeTest extends AbstractProtocolTest {

    @SuppressWarnings("serial")
	@Override
    protected MockCommunications newMember(int port, Color initialColor, int cardinality) {
        SnowflakeParameters parameters = new SnowflakeParameters();
        parameters.alpha = 0.9f;
        parameters.beta = 5;
        parameters.interval = 100l;
        parameters.intervalUnit = TimeUnit.MILLISECONDS;
        parameters.retries = 1;
        parameters.sample = cardinality / 2;
        parameters.timeout = 200;
        parameters.unit = TimeUnit.MILLISECONDS;
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        Member member = mock(Member.class);
        MockCommunications<Snowball<Color>> communications = new MockCommunications<>(member, scheduler);
        communications.setProtocol(new Snowball<Color>(communications, parameters, new Random(0x666),
                                                       new ArrayList<Color>() {
                                                           {
                                                               add(Color.Red);
                                                               add(Color.Blue);
                                                           }
                                                       }, scheduler, initialColor));
        return communications;
    }
}
