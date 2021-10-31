/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.membership.impl.MemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TxDataSourceTest {

    @Test
    public void func() {
        TxDataSource ds = new TxDataSource(new MemberImpl(Utils.getMember(0).getX509Certificate()), 100, null, 1024,
                                           Duration.ofMillis(100), 100);
        Transaction tx = Transaction.newBuilder()
                                    .setContent(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                    .build();
        int count = 0;
        while (ds.offer(tx)) {
            count++;
        }
        assertEquals(155, count);
        assertEquals(5, ds.getProcessing());

        var data = ds.getData();
        assertNotNull(data);
        assertEquals(1144, data.size());

        assertTrue(ds.offer(tx));

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1144, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1144, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1144, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1144, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1100, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(0, data.size());
    }
}
