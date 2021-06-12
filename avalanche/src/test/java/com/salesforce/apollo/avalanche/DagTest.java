/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
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

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.ByteMessage;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.DagEntry.Builder;
import com.salesfoce.apollo.proto.DagEntry.EntryType;
import com.salesforce.apollo.avalanche.Processor.NullProcessor;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class DagTest {

    private static File baseDir;
    private WorkingSet  workingSet;
    private Random      entropy;
    private DagEntry    root;
    private Digest     rootKey;

    @BeforeAll
    public static void beforeClass() {
        baseDir = new File(System.getProperty("user.dir"), "target/dag-tst");
        Utils.clean(baseDir);
        baseDir.mkdirs();
    }

    @AfterEach
    public void after() {
    }

    public static DagEntry dag(byte[] data) {
        return dag(EntryType.GENSIS, data, Collections.emptyList());
    }

    public static DagEntry dag(byte[] data, List<Digest> links) {
        return dag(EntryType.USER, data, links);
    }

    public static DagEntry dag(EntryType type, byte[] data, List<Digest> links) {
        Builder builder = DagEntry.newBuilder();
        builder.setDescription(type);
        builder.setData(Any.pack(ByteMessage.newBuilder().setContents(ByteString.copyFrom(data)).build()));
        links.forEach(e -> builder.addLinks(qb64(e)));
        return builder.build();
    }

    @BeforeEach
    public void before() throws SQLException {
        entropy = new Random(0x666);
        final AvalancheParameters parameters = new AvalancheParameters();
        workingSet = new WorkingSet(new NullProcessor(), parameters, new MVStore.Builder().open().openMap("Test"),
                null);
        root = dag("Ye root".getBytes());
        rootKey = workingSet.insert(root, 0);
        assertNotNull(rootKey);
    }

    @Test
    public void smoke() throws Exception {
        DagEntry testRoot = workingSet.getDagEntry(rootKey);
        assertNotNull(testRoot);
        assertArrayEquals(root.getData().toByteArray(), testRoot.getData().toByteArray());
        assertEquals(0, testRoot.getLinksCount());

        List<Digest> ordered = new ArrayList<>();
        ordered.add(rootKey);

        Map<Digest, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(rootKey, root);

        for (int i = 0; i < 500; i++) {
            DagEntry entry = dag(EntryType.USER, String.format("DagEntry: %s", i).getBytes(), randomLinksTo(stored));
            Digest key = workingSet.insert(entry, 0);
            stored.put(key, entry);
            ordered.add(key);
        }
        assertEquals(501, stored.size());

        assertEquals(501, workingSet.getUnfinalized().size());

        for (Digest key : ordered) {
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

    private List<Digest> randomLinksTo(Map<Digest, DagEntry> stored) {
        List<Digest> links = new ArrayList<>();
        Set<Digest> keys = stored.keySet();
        for (int i = 0; i < 5; i++) {
            Iterator<Digest> it = keys.iterator();
            for (int j = 0; j < entropy.nextInt(keys.size()); j++) {
                it.next();
            }
            links.add(it.next());
        }
        return links;
    }
}
