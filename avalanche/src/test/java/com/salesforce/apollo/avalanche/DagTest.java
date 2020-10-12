/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.DagEntry.Builder;
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

    @BeforeAll
    public static void beforeClass() {
        baseDir = new File(System.getProperty("user.dir"), "target/dag-tst");
        Utils.clean(baseDir);
        baseDir.mkdirs();
    }

    @AfterEach
    public void after() {
    }

    public static DagEntry dag(HashKey description, byte[] data) {
        return dag(description, data, Collections.emptyList());
    }

    public static DagEntry dag(HashKey description, byte[] data, List<HashKey> links) {
        Builder builder = DagEntry.newBuilder();
        if (description != null)
            builder.setDescription(description.toByteString());
        builder.setData(ByteString.copyFrom(data));
        links.forEach(e -> builder.addLinks(e.toByteString()));
        return builder.build();
    }

    @BeforeEach
    public void before() throws SQLException {
        entropy = new Random(0x666);
        final AvalancheParameters parameters = new AvalancheParameters();
        workingSet = new WorkingSet(parameters, new DagWood(parameters.dagWood), null);
        root = dag(WellKnownDescriptions.GENESIS.toHash(), "Ye root".getBytes());
        rootKey = workingSet.insert(root, 0);
        assertNotNull(rootKey);
    }

    @Test
    public void smoke() throws Exception {
        DagEntry testRoot = workingSet.getDagEntry(rootKey);
        assertNotNull(testRoot);
        assertArrayEquals(root.getData().toByteArray(), testRoot.getData().toByteArray());
        assertEquals(0, testRoot.getLinksCount());

        List<HashKey> ordered = new ArrayList<>();
        ordered.add(rootKey);

        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(rootKey, root);

        for (int i = 0; i < 500; i++) {
            DagEntry entry = dag(WellKnownDescriptions.BYTE_CONTENT.toHash(),
                                 String.format("DagEntry: %s", i).getBytes(), randomLinksTo(stored));
            HashKey key = workingSet.insert(entry, 0);
            stored.put(key, entry);
            ordered.add(key);
        }
        assertEquals(501, stored.size());

        assertEquals(501, workingSet.getUnfinalized().size());

        for (HashKey key : ordered) {
            assertEquals(1, workingSet.getConflictSet(key).getCardinality());
            DagEntry found = workingSet.getDagEntry(key);
            assertNotNull(found, "Not found: " + key);
            DagEntry original = stored.get(key);
            assertArrayEquals(original.getData().toByteArray(), found.getData().toByteArray());
            if (original.getLinksList() == null) {
                assertNull(found.getLinksList());
            } else {
                assertEquals(original.getLinksList().size(), found.getLinksList().size());
            }
        }
    }

    private List<HashKey> randomLinksTo(Map<HashKey, DagEntry> stored) {
        List<HashKey> links = new ArrayList<>();
        Set<HashKey> keys = stored.keySet();
        for (int i = 0; i < 5; i++) {
            Iterator<HashKey> it = keys.iterator();
            for (int j = 0; j < entropy.nextInt(keys.size()); j++) {
                it.next();
            }
            links.add(it.next());
        }
        return links;
    }
}
