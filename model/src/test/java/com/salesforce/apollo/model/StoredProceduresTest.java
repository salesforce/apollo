/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static com.salesforce.apollo.model.schema.tables.Member.MEMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.xml.bind.DatatypeConverter;

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
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        Duration timeout = Duration.ofSeconds(100);
        Executor exec = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        Emulator emmy = new Emulator();
        emmy.start(Domain.boostrapMigration());

        var ids = new ArrayList<SelfAddressingIdentifier>();
        for (int i = 0; i < 100; i++) {
            ids.add(new SelfAddressingIdentifier(DigestAlgorithm.DEFAULT.random(entropy)));
        }

        var call = emmy.getMutator()
                       .call("{call apollo_kernel.add_members(?, ?) }",
                             ids.stream().map(d -> d.getDigest().getBytes()).toList(), "active");

        var result = emmy.getMutator().execute(exec, call, timeout, scheduler);
        result.get();

        var connector = emmy.newConnector();
        var context = DSL.using(connector, SQLDialect.H2);

        var members = context.selectFrom(MEMBER).fetch();
        assertEquals(ids.size(), members.size());

        DatatypeConverter.class.toGenericString();
        for (var digest : ids) {
            assertTrue(Domain.isMember(context, digest), "Not an active member: " + digest);
        }
    }
}
