/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class TxDataSourceTest {

    @Test
    public void func() {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 9); 
        var parameters = Parameters.newBuilder()
                                   .setScheduler(exec)
                                   .setContext(context)
                                   .setProducer(ProducerParameters.newBuilder().setMaxBatchByteSize(1024).build())
                                   .build();
        TxDataSource ds = new TxDataSource(parameters, 1024 * 5);
        Transaction tx = Transaction.newBuilder()
                                    .setContent(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                    .build();
        int count = 0;
        while (ds.offer(tx)) {
            count++;
        }
        assertEquals(121, count);
        assertEquals(121, ds.getProcessing());
        assertEquals(38, ds.getRemaining());
        assertEquals(5082, ds.getBuffered());

        var data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());
        assertEquals(-986, ds.getRemaining());
        assertEquals(4074, ds.getBuffered());

        assertFalse(ds.offer(tx));

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());
        assertEquals(-2010, ds.getRemaining());
        assertEquals(3066, ds.getBuffered());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());
        assertEquals(-3034, ds.getRemaining());
        assertEquals(2058, ds.getBuffered());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());
        assertEquals(-4058, ds.getRemaining());
        assertEquals(1050, ds.getBuffered());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());
        assertEquals(-5082, ds.getRemaining());
        assertEquals(42, ds.getBuffered());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(44, data.size());
        assertEquals(-6106, ds.getRemaining());
        assertEquals(0, ds.getBuffered());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(0, data.size());
        assertEquals(-6106, ds.getRemaining());
        assertEquals(0, ds.getBuffered());
    }
}
