/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static com.salesforce.apollo.model.schema.tables.Member.MEMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.state.Emulator;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class StoredProceduresTest {

    @Test
    public void membership() throws Exception {
        var entropy = new Random(0x1638);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Duration timeout = Duration.ofSeconds(100);
        Executor exec = Executors.newSingleThreadExecutor();
        Emulator emmy = new Emulator();
        emmy.start(Node.boostrapMigration());

        var digests = new ArrayList<byte[]>();
        for (int i = 0; i < 100; i++) {
            digests.add(new SelfAddressingIdentifier(DigestAlgorithm.DEFAULT.random(entropy)).toIdent().toByteArray());
        }

        var call = emmy.getMutator().call("{call apollo_kernel.add_members(?) }", digests);

        var result = emmy.getMutator().execute(exec, call, timeout, scheduler);
        result.get();

        var connector = emmy.newConnector();
        var context = DSL.using(connector, SQLDialect.H2);

        var members = context.selectFrom(MEMBER).fetch();
        assertEquals(digests.size(), members.size());
        // TODO, full testing
    }
}
