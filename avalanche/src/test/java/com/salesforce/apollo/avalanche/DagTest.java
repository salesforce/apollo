/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.dagwood.schema.Tables.CLOSURE;
import static com.salesforce.apollo.dagwood.schema.Tables.CONFLICTSET;
import static com.salesforce.apollo.dagwood.schema.Tables.DAG;
import static com.salesforce.apollo.dagwood.schema.Tables.UNFINALIZED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.dagwood.schema.tables.records.UnfinalizedRecord;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class DagTest {

    private static final String CONNECTION_URL = "jdbc:h2:mem:test";
    private Connection          connection;
    private DSLContext          create;
    private WorkingSet          workingSet;
    private Random              entropy;
    private DagEntry            root;
    private HashKey             rootKey;

    @After
    public void after() {
        if (create != null) {
            create.close();
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    @Before
    public void before() throws SQLException {
        Avalanche.loadSchema(CONNECTION_URL);
        connection = DriverManager.getConnection(CONNECTION_URL, "apollo", "");
        connection.setAutoCommit(false);
        ConnectionProvider provider = new DefaultConnectionProvider(connection);
        create = DSL.using(provider, SQLDialect.H2);
        create.deleteFrom(DAG).execute();
        create.deleteFrom(CLOSURE).execute();
        create.deleteFrom(CONFLICTSET).execute();
        entropy = new Random(0x666);
        workingSet = new WorkingSet(new AvalancheParameters());
        root = new DagEntry();
        root.setDescription(WellKnownDescriptions.GENESIS.toHash());
        root.setData(ByteBuffer.wrap("Ye root".getBytes()));
        rootKey = workingSet.insert(root, 0, create);
        assertNotNull(rootKey);
    }

    @Test
    public void smoke() throws Exception {
        DagEntry testRoot = workingSet.get(rootKey, create);
        assertNotNull(testRoot);
        testRoot.setDescription(WellKnownDescriptions.GENESIS.toHash());
        assertNotNull(testRoot);
        assertArrayEquals(root.getData().array(), testRoot.getData().array());
        assertNull(testRoot.getLinks());

        List<HashKey> ordered = new ArrayList<>();
        ordered.add(rootKey);

        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(rootKey, root);

        for (int i = 0; i < 500; i++) {
            DagEntry entry = new DagEntry();
            entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
            entry.setData(ByteBuffer.wrap(String.format("DagEntry: %s", i).getBytes()));
            entry.setLinks(randomLinksTo(stored));
            HashKey key = workingSet.insert(entry, 0, create);
            stored.put(key, entry);
            ordered.add(key);
        }
        assertEquals(501, stored.size());

        Result<UnfinalizedRecord> records = create.selectFrom(UNFINALIZED).fetch();
        assertEquals(501, records.size());

        for (HashKey key : ordered) {
            assertEquals(1, workingSet.getConflictSet(key).getCardinality());
            DagEntry found = workingSet.get(key, create);
            assertNotNull("Not found: " + key, found);
            DagEntry original = stored.get(key);
            assertArrayEquals(original.getData().array(), found.getData().array());
            if (original.getLinks() == null) {
                assertNull(found.getLinks());
            } else {
                assertEquals(original.getLinks().size(), found.getLinks().size());
            }
        }
    }

    private List<HASH> randomLinksTo(Map<HashKey, DagEntry> stored) {
        List<HASH> links = new ArrayList<>();
        Set<HashKey> keys = stored.keySet();
        for (int i = 0; i < 5; i++) {
            Iterator<HashKey> it = keys.iterator();
            for (int j = 0; j < entropy.nextInt(keys.size()); j++) {
                it.next();
            }
            links.add(it.next().toHash());
        }
        return links;
    }
}
