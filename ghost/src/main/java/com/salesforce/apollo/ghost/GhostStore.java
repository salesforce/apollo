/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class GhostStore implements Store {
    private final static String IMMUTABLE_MAP_TEMPLATE = "%s.immutable.ghostStore";
    private final static Logger log                    = LoggerFactory.getLogger(GhostStore.class);
    private final static String MUTABLE_MAP_TEMPLATE   = "%s.mutable.ghostStore";

    private final MVMap<Digest, byte[]> bindings;
    private final MVMap<Digest, byte[]> contents;
    private final DigestAlgorithm       digestAlgorithm;

    public GhostStore(Digest id, DigestAlgorithm digestAlgorithm, MVStore store) {
        this(store.openMap(String.format(MUTABLE_MAP_TEMPLATE, id)), digestAlgorithm,
                store.openMap(String.format(IMMUTABLE_MAP_TEMPLATE, id)));
    }

    public GhostStore(MVMap<Digest, byte[]> mutable, DigestAlgorithm digestAlgorithm, MVMap<Digest, byte[]> immutable) {
        this.contents = immutable;
        this.bindings = mutable;
        this.digestAlgorithm = digestAlgorithm;
    }

    @Override
    public void add(List<Content> content) {
        content.forEach(e -> {
            var key = digestAlgorithm.digest(e.toByteString());
            contents.put(key, e.toByteArray());
        });
    }

    @Override
    public void bind(Digest key, Binding binding) {
        bindings.put(key, binding.toByteArray());
    }

    @Override
    public Entries entriesIn(CombinedIntervals combined, int maxEntries) {
        Entries.Builder builder = Entries.newBuilder();
        for (KeyInterval interval : combined.getIntervals()) {
            contentsIn(maxEntries, builder, interval);
            bindingsIn(maxEntries, builder, interval);
        }
        return builder.build();
    }

    @Override
    public Content get(Digest key) {
        byte[] value = contents.get(key);
        try {
            return value == null ? Content.getDefaultInstance() : Content.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            log.debug("Unable to deserialize: {}", key);
            throw new IllegalStateException("Unable to deserialize contents for key: " + key);
        }
    }

    @Override
    public Binding lookup(Digest key) {
        byte[] value = bindings.get(key);
        try {
            return value == null ? Binding.getDefaultInstance() : Binding.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            log.debug("Unable to deserialize: {}", key);
            throw new IllegalStateException("Unable to deserialize binding for key: " + key);
        }
    }

    @Override
    public void populate(CombinedIntervals combined, double fpr, SecureRandom entropy) {
        combined.getIntervals().forEach(interval -> {
            interval.setContentsBff(populateImmutable(fpr, entropy, interval));
            interval.setBindingsBff(populateMutable(fpr, entropy, interval));
        });
    }

    @Override
    public void purge(Digest key) {
        contents.remove(key);
    }

    @Override
    public void put(Digest key, Content content) {
        if (contents.get(key) == null) {
            contents.putIfAbsent(key, content.toByteArray());
        }
    }

    @Override
    public void remove(Digest key) {
        bindings.remove(key);
    }

    private void contentsIn(int maxEntries, Entries.Builder builder, KeyInterval interval) {
        Cursor<Digest, byte[]> cursor = new Cursor<Digest, byte[]>(contents.getRootPage(), interval.getBegin(),
                interval.getEnd());
        while (cursor.hasNext()) {
            Digest key = cursor.next();
            if (!interval.bindingsContains(key)) {
                try {
                    byte[] content = contents.get(key);
                    if (content != null) {
                        builder.addContent(Content.parseFrom(content));
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.debug("Unable to deserialize contents: {}", key);
                }
                if (builder.getContentCount() >= maxEntries) {
                    break;
                }
            }
        }
    }

    private void bindingsIn(int maxEntries, Entries.Builder builder, KeyInterval interval) {
        Cursor<Digest, byte[]> cursor = new Cursor<Digest, byte[]>(bindings.getRootPage(), interval.getBegin(),
                interval.getEnd());
        while (cursor.hasNext()) {
            Digest key = cursor.next();
            if (!interval.bindingsContains(key)) {
                try {
                    byte[] binding = bindings.get(key);
                    if (binding != null) {
                        builder.addBinding(Binding.parseFrom(binding));
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.debug("Unable to deserialize binding: {}", key);
                }
                if (builder.getBindingCount() >= maxEntries) {
                    break;
                }
            }
        }
    }

    private BloomFilter<Digest> populateImmutable(double fpr, SecureRandom entropy, KeyInterval interval) {
        List<Digest> subSet = new ArrayList<>();
        new Cursor<Digest, byte[]>(contents.getRootPage(), interval.getBegin(),
                interval.getEnd()).forEachRemaining(key -> subSet.add(key));
        BloomFilter<Digest> bff = new DigestBloomFilter(entropy.nextLong(), subSet.size(), fpr);
        subSet.forEach(h -> bff.add(h));
        return bff;
    }

    private BloomFilter<Digest> populateMutable(double fpr, SecureRandom entropy, KeyInterval interval) {
        List<Digest> subSet = new ArrayList<>();
        new Cursor<Digest, byte[]>(bindings.getRootPage(), interval.getBegin(),
                interval.getEnd()).forEachRemaining(key -> subSet.add(key));
        BloomFilter<Digest> bff = new DigestBloomFilter(entropy.nextLong(), subSet.size(), fpr);
        subSet.forEach(h -> bff.add(h));
        return bff;
    }

}
