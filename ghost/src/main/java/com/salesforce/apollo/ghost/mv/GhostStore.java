/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.mv;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ghost.CombinedIntervals;
import com.salesforce.apollo.ghost.KeyInterval;
import com.salesforce.apollo.ghost.Store;
import com.salesforce.apollo.utils.DigestType;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * Implementation of the Ghost Store using the most excellent MVStore. Provides
 * large scale - up to 7TB - of log structured merge storage with excellent
 * caching and performance.
 * 
 * @author hal.hildebrand
 *
 */
public class GhostStore implements Store {
    private final static String BINDINGS_MAP_TEMPLATE = "%s.bindings.ghostStore";
    private final static String CONTENTS_MAP_TEMPLATE = "%s.contents.ghostStore";
    @SuppressWarnings("unused")
    private final static Logger log                   = LoggerFactory.getLogger(GhostStore.class);

    private final MVMap<Digest, Binding> bindings;
    private final MVMap<Digest, Content> contents;
    private final DigestAlgorithm        digestAlgorithm;

    public GhostStore(Digest id, DigestAlgorithm digestAlgorithm, MVStore store) {
        this.digestAlgorithm = digestAlgorithm;

        DigestType digestType = new DigestType();
        this.contents = store.openMap(String.format(CONTENTS_MAP_TEMPLATE, id),
                                      new MVMap.Builder<Digest, Content>().keyType(digestType)
                                                                          .valueType(new ContentType()));

        this.bindings = store.openMap(String.format(BINDINGS_MAP_TEMPLATE, id),
                                      new MVMap.Builder<Digest, Binding>().keyType(digestType)
                                                                          .valueType(new BindingType()));
    }

    @Override
    public void add(List<Content> content) {
        content.forEach(e -> {
            var key = digestAlgorithm.digest(e.toByteString());
            contents.put(key, e);
        });
    }

    @Override
    public void bind(Digest key, Binding binding) {
        bindings.put(key, binding);
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
        Content content = contents.get(key);
        return content == null ? Content.getDefaultInstance() : content;
    }

    @Override
    public Binding lookup(Digest key) {
        Binding binding = bindings.get(key);
        return binding == null ? Binding.getDefaultInstance() : binding;
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
            contents.putIfAbsent(key, content);
        }
    }

    @Override
    public void remove(Digest key) {
        bindings.remove(key);
    }

    private void bindingsIn(int maxEntries, Entries.Builder builder, KeyInterval interval) {
        Cursor<Digest, byte[]> cursor = new Cursor<Digest, byte[]>(bindings.getRootPage(), interval.getBegin(),
                                                                   interval.getEnd());
        while (cursor.hasNext()) {
            Digest key = cursor.next();
            if (!interval.bindingsContains(key)) {
                Binding binding = bindings.get(key);
                if (binding != null) {
                    builder.addBinding(binding);
                }
                if (builder.getBindingCount() >= maxEntries) {
                    break;
                }
            }
        }
    }

    private void contentsIn(int maxEntries, Entries.Builder builder, KeyInterval interval) {
        Cursor<Digest, byte[]> cursor = new Cursor<Digest, byte[]>(contents.getRootPage(), interval.getBegin(),
                                                                   interval.getEnd());
        while (cursor.hasNext()) {
            Digest key = cursor.next();
            if (!interval.bindingsContains(key)) {
                Content content = contents.get(key);
                if (content != null) {
                    builder.addContent(content);
                }
                if (builder.getContentCount() >= maxEntries) {
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
