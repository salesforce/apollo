/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class DagTest {

    private static File baseDir; 
    private WorkingSet  workingSet;
    private Random      entropy;
    private DagEntry    root;
    private HashKey     rootKey;

    @BeforeClass
    public static void beforeClass() {
        baseDir = new File(System.getProperty("user.dir"), "target/dag-tst");
        Utils.clean(baseDir);
        baseDir.mkdirs();
    }

    @After
    public void after() { 
    }

    @Before
    public void before() throws SQLException {
        entropy = new Random(0x666);
        final AvalancheParameters parameters = new AvalancheParameters(); 
        workingSet = new WorkingSet(parameters, new DagWood(parameters.dagWood), null);
        root = new DagEntry();
        root.setDescription(WellKnownDescriptions.GENESIS.toHash());
        root.setData(ByteBuffer.wrap("Ye root".getBytes()));
        rootKey = workingSet.insert(root, 0);
        assertNotNull(rootKey);
    }

    @Test
    public void smoke() throws Exception {
        DagEntry testRoot = workingSet.getDagEntry(rootKey);
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
            HashKey key = workingSet.insert(entry, 0);
            stored.put(key, entry);
            ordered.add(key);
        }
        assertEquals(501, stored.size());

        assertEquals(501, workingSet.getUnfinalized().size());

        for (HashKey key : ordered) {
            assertEquals(1, workingSet.getConflictSet(key).getCardinality());
            DagEntry found = workingSet.getDagEntry(key);
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
