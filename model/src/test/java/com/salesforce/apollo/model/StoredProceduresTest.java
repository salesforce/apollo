/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.state.Emulator;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import javax.xml.bind.DatatypeConverter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.salesforce.apollo.model.schema.tables.Member.MEMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 */
public class StoredProceduresTest {

    @Test
    public void membership() throws Exception {
        var entropy = new Random(0x1638);
        Duration timeout = Duration.ofSeconds(100);
        Emulator emmy = new Emulator();
        emmy.start(Domain.boostrapMigration());

        var ids = new ArrayList<SelfAddressingIdentifier>();
        for (int i = 0; i < 100; i++) {
            ids.add(new SelfAddressingIdentifier(DigestAlgorithm.DEFAULT.random(entropy)));
        }

        var call = emmy.getMutator()
                       .call("{call apollo_kernel.add_members(?, ?) }",
                             ids.stream().map(d -> d.getDigest().getBytes()).toList(), "active");

        var result = emmy.getMutator().execute(call, timeout);
        result.get();

        var connector = emmy.newConnector();
        var context = DSL.using(connector, SQLDialect.H2);

        var members = context.selectFrom(MEMBER).fetch();
        assertEquals(ids.size(), members.size());

        for (var digest : ids) {
            assertTrue(Domain.isMember(context, digest), "Not an active member: " + digest);
        }
    }
}
