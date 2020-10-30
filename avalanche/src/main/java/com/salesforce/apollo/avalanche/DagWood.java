/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hhildebrand
 *
 *         Manages the multi storage tier DAG db for Apollo
 */
public class DagWood {

    private static final HashKeySerializer SERIALIZER = new HashKeySerializer();

    public static class HashKeySerializer implements Serializer<HashKey> {

        @Override
        public void serialize(DataOutput2 out, HashKey value) throws IOException {
            for (Long l : value.longs()) {
                out.writeLong(l);
            }
        }

        @Override
        public HashKey deserialize(DataInput2 input, int available) throws IOException {
            long[] itself = new long[4];
            for (int i = 0; i < 4; i++) {
                itself[i] = input.readLong();
            }
            return new HashKey(itself);
        }

        @Override
        public int fixedSize() {
            return 8 * 4;
        }

        @Override
        public boolean isTrusted() {
            return true;
        }

        @Override
        public int compare(HashKey first, HashKey second) {
            return first.compareTo(second);
        }

    }

    public static class DagWoodParameters {
        public long maxCache = 150_000;
        public File file;

        public DagWoodParameters() {
            try {
                file = File.createTempFile("dagwood", "ws");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static final String CACHE = "dagwood.cache";

    private final HTreeMap<HashKey, byte[]> cache;
    private final DB                        db;

    public DagWood(DagWoodParameters parameters) {
        try {
            parameters.file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        db = DBMaker.tempFileDB()
                    .fileMmapEnable() // Always enable mmap
                    .fileMmapEnableIfSupported() // Only enable mmap on supported platforms
                    .fileMmapPreclearDisable() // Make mmap file faster
                    // Unmap (release resources) file when its closed.
                    // That can cause JVM crash if file is accessed after it was unmapped
                    // (there is possible race condition).
                    .cleanerHackEnable()
                    .allocateStartSize(10 * 1024 * 1024 * 1024) // 10GB
                    .allocateIncrement(512 * 1024 * 1024) // 512MB
                    .make();

        // optionally preload file content into disk cache
//        db.getStore().fileLoad();

        cache = db.hashMap(CACHE)
                  .keySerializer(SERIALIZER)
                  .valueSerializer(Serializer.BYTE_ARRAY)
                  .expireAfterCreate()
                  .counterEnable()
                  .createOrOpen();
    }

    public List<HashKey> allFinalized() {
        List<HashKey> all = new ArrayList<>();
        cache.keySet().forEach(e -> all.add(e));
        return all;
    }

    public boolean cacheContainsKey(HashKey key) {
        return cache.containsKey(key);
    }

    public void close() {
        db.close();
    }

    public boolean containsKey(HashKey key) {
        return cache.containsKey(key);
    }

    public byte[] get(HashKey key) {
        return cache.get(key);

    }

    public Set<HashKey> keySet() {
        return cache.keySet();
    }

    public void put(HashKey key, byte[] entry) {
        assert key != null : "Must have non null key";
        assert entry.length > 0 : "Must have >0 byte[] entry";
        cache.putIfAbsent(key, entry);
    }

    public int size() {
        return cache.getSize();
    }
}
