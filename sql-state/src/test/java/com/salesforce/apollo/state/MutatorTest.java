/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.nio.file.Path;
import java.sql.JDBCType;
import java.sql.Types;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.joou.ULong;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Migration;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.Session;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.state.SqlStateMachine.CallResult;

/**
 * @author hal.hildebrand
 *
 */
public class MutatorTest {
    public static final Path   MUT_RESOURCE_PATH = Path.of("src", "test", "resources", "mutator-testing");
    public static final String MUT_SCHEMA_ROOT   = "mutator-testing.xml";

    private static int TEST_VALUE;

    public static int callTest() {
        return TEST_VALUE;
    }

    @Test
    public void smokin() throws Exception {
        SqlStateMachine updater = new SqlStateMachine("jdbc:h2:mem:smokin;DATABASE_TO_UPPER=TRUE", new Properties(),
                                                      new File("target/chkpoints"));
        final var executor = updater.getExecutor();
        executor.genesis(ULong.valueOf(0), DigestAlgorithm.DEFAULT.getLast(), Collections.emptyList());

        Migration migration = Migration.newBuilder()
                                       .setUpdate(Mutator.changeLog(MigrationTest.BOOK_RESOURCE_PATH,
                                                                    MigrationTest.BOOK_SCHEMA_ROOT))
                                       .build();

        CompletableFuture<Object> success = new CompletableFuture<>();
        executor.execute(0, Digest.NONE,
                         Transaction.newBuilder()
                                    .setContent(Txn.newBuilder().setMigration(migration).build().toByteString())
                                    .build(),
                         success);

        success.get(1, TimeUnit.SECONDS);

        migration = Migration.newBuilder().setUpdate(Mutator.changeLog(MUT_RESOURCE_PATH, MUT_SCHEMA_ROOT)).build();
        success = new CompletableFuture<>();
        executor.execute(1, Digest.NONE,
                         Transaction.newBuilder()
                                    .setContent(Txn.newBuilder().setMigration(migration).build().toByteString())
                                    .build(),
                         success);

        success.get(1, TimeUnit.SECONDS);

        var connection = updater.newConnection();

        var sql = "{ ? = call mut.testCall() }";
        var proc = connection.prepareCall(sql);
        proc.registerOutParameter(1, Types.INTEGER);
        TEST_VALUE = 0x1638;
        proc.execute();
        assertEquals(0x1638, proc.getInt(1));

        var session = mock(Session.class);
        var mutator = updater.getMutator(session);

        TEST_VALUE = 0x1637;
        var call = mutator.call(sql, Collections.singletonList(JDBCType.INTEGER));

        success = new CompletableFuture<>();
        executor.execute(1, Digest.NONE,
                         Transaction.newBuilder()
                                    .setContent(Txn.newBuilder().setCall(call).build().toByteString())
                                    .build(),
                         success);

        CallResult result = (CallResult) success.get(1, TimeUnit.SECONDS);
        assertNotNull(result);
        assertEquals(Integer.valueOf(0x1637), result.get(0));
    }
}
