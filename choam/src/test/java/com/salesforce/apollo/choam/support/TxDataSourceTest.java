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

import java.security.SecureRandom;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class TxDataSourceTest {

    @Test
    public void func() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        TxDataSource ds = new TxDataSource(new ControlledIdentifierMember(stereotomy.newIdentifier()), 100, null,
                                           1024, Duration.ofMillis(100), 100,
                                           ExponentialBackoffPolicy.newBuilder().build());
        Transaction tx = Transaction.newBuilder()
                                    .setContent(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                    .build();
        int count = 0;
        while (ds.offer(tx)) {
            count++;
        }
        assertEquals(2400, count);
        assertEquals(2400, ds.getRemainingTransactions());

        var data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());

        assertFalse(ds.offer(tx));

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(1056, data.size());

        for (int i = 0; i < 94; i++) {
            data = ds.getData();
            assertNotNull(data);
            assertEquals(1056, data.size());
        }

        assertEquals(0, ds.getRemainingTransactions());

        data = ds.getData();
        assertNotNull(data);
        assertEquals(0, data.size());
    }
}
