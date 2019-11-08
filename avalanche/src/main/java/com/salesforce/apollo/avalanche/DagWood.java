/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Executors;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.google.common.collect.Sets;

/**
 * @author hhildebrand
 *
 *         Manages the multi storage tier DAG db for Apollo
 */
public class DagWood {

    public static class DagWoodParameters {

        public long maxCache = 50_000;
        public File store    = new File("dagwood.store");

    }

    private static final String CACHE = "dagwood.cache";
    private static final String STORE = "wood";

    private final HTreeMap<byte[], byte[]> cache;
    private final DB                       dbDisk;
    private final DB                       dbMemory;
    private final DagWoodParameters        parameters;
    private final BTreeMap<byte[], byte[]> wood;

    public DagWood(DagWoodParameters p) {
        parameters = p;
        dbMemory = DBMaker.memoryDB().make();
        dbDisk = DBMaker.fileDB(parameters.store).fileMmapEnable().fileMmapPreclearDisable().cleanerHackEnable().make();

        wood = dbDisk.treeMap(STORE)
                     .keySerializer(Serializer.BYTE_ARRAY)
                     .valueSerializer(Serializer.BYTE_ARRAY)
                     .counterEnable()
                     .create();

        cache = dbMemory.hashMap(CACHE)
                        .keySerializer(Serializer.BYTE_ARRAY)
                        .valueSerializer(Serializer.BYTE_ARRAY)
                        .expireAfterCreate()
                        .expireExecutor(Executors.newScheduledThreadPool(2))
                        .expireOverflow(wood)
                        .expireMaxSize(parameters.maxCache)
                        .counterEnable()
                        .createOrOpen();
        dbDisk.getStore().fileLoad();
    }

    public void close() {
        dbDisk.close();
        dbMemory.close();
        wood.close();
    }

    public boolean cacheContainsKey(byte[] key) {
        return cache.containsKey(key);
    }

    public boolean containsKey(byte[] key) {
        return cache.containsKey(key) || wood.containsKey(key);
    }

    public byte[] get(byte[] key) {
        byte[] value = cache.get(key);
        if (value != null) {
            return value;
        }
        return wood.get(key);
    }

    public Set<byte[]> keySet() {
        return Sets.union(cache.keySet(), wood.keySet());
    }

    public void put(byte[] key, byte[] entry) {
        cache.putIfAbsent(key, entry);
    }

    public int size() {
        return cache.getSize() + wood.getSize();
    }
}
