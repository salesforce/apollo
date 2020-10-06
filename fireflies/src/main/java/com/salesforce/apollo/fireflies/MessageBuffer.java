/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.security.SignatureException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.Message;
import com.salesfoce.apollo.proto.MessageDigest;
import com.salesfoce.apollo.proto.MessageGossip;
import com.salesfoce.apollo.proto.MessageGossip.Builder;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.BloomFilter.HashFunction;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MessageBuffer {
    private final static Logger log = LoggerFactory.getLogger(MessageBuffer.class);

    private final int                   bufferSize;
    private final Map<HashKey, Long>    maxTimes = new ConcurrentHashMap<>();
    private final Map<HashKey, Message> state    = new ConcurrentHashMap<>();
    private final int                   tooOld;

    public MessageBuffer(int bufferSize, int tooOld) {
        this.bufferSize = bufferSize;
        this.tooOld = tooOld;
    }

    public void clear() {
        maxTimes.clear();
        state.clear();
    }

    public void gc() {
        if (state.size() > bufferSize) {
            compact();
        }
    }

    public BloomFilter getBff(HashKey seed, double p) {
        BloomFilter bff = new BloomFilter(new HashFunction(seed, bufferSize, p));
        state.keySet().forEach(h -> bff.add(h));
        return bff;
    }

    /**
     * @return the digest state
     */
    public List<MessageDigest> getDigests() {
        return state.values().stream().map(m -> m.getDigest()).collect(Collectors.toList());
    }

    /**
     * Merge the updates.
     * 
     * @param updates
     * @param validator
     * @return the list of new messages for this buffer
     */
    public List<Message> merge(List<Message> updates, Predicate<Message> validator) {
        return updates.stream().filter(validator).filter(message -> put(message)).collect(Collectors.toList());
    }

    public MessageGossip process(List<MessageDigest> requested, HashKey seed, double p) {
        log.trace("process message digests: ", requested.size());
        Builder builder = MessageGossip.newBuilder();
        Set<HashKey> received = new HashSet<>(requested.size());
        requested.stream().filter(d -> {
            HashKey id = new HashKey(d.getId());
            received.add(id);
            return null == state.computeIfPresent(id,
                                                  (k,
                                                   m) -> Message.newBuilder(m)
                                                                .setDigest(MessageDigest.newBuilder(m.getDigest())
                                                                                        .setAge(Math.max(m.getDigest()
                                                                                                          .getAge(),
                                                                                                         d.getAge()))
                                                                                        .build())
                                                                .build());
        }).forEach(e -> builder.addDigests(e));

        Sets.difference(state.keySet(), received).stream().map(id -> state.get(id)).forEach(e -> builder.addUpdates(e));
        builder.setBff(getBff(seed, p).toBff());
        MessageGossip gossip = builder.build();

        log.trace("want messages: {} updates: {}", gossip.getDigestsCount(), gossip.getUpdatesCount());
        return gossip;
    }

    /**
     * Insert a new message into the buffer from the node
     * 
     * @param ts
     * @param bytes
     * @param from
     * @param channel
     * @return the inserted Message
     */
    public Message put(long ts, byte[] bytes, Node from, int channel) {
        ByteBuffer header = ByteBuffer.allocate(8 + 4);
        header.putLong(ts);
        header.putInt(channel);
        HashKey id = new HashKey(Conversion.hashOf(from.getId().bytes(), header.array(), bytes));
        Message update = createUpdate(channel, id, ts, bytes, from.forSigning(), from.getId());
        put(update);
        log.trace("broadcasting: {}", id);
        return update;
    }

    public List<Message> updatesFor(List<MessageDigest> digests) {
        return digests.stream()
                      .map(digest -> state.get(new HashKey(digest.getId())))
                      .filter(m -> m != null)
                      .collect(Collectors.toList());
    }

    private void compact() {
        log.trace("Compacting buffer");
        removeOutOfDate();
        purgeTheAged();
    }

    private Message createUpdate(int channel, HashKey id, long ts, byte[] content, Signature signature, HashKey from) {
        byte[] s;
        try {
            signature.update(content);
            s = signature.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign message content", e);
        }
        return Message.newBuilder()
                      .setDigest(MessageDigest.newBuilder()
                                              .setSource(from.toByteString())
                                              .setId(id.toByteString())
                                              .setAge(0)
                                              .setTime(System.currentTimeMillis())
                                              .build())
                      .setChannel(channel)
                      .setContent(ByteString.copyFrom(content))
                      .setSignature(ByteString.copyFrom(s))
                      .build();
    }

    private void purgeTheAged() {
        Message max;
        while (state.size() > bufferSize) {
            max = null;
            for (Message u : state.values()) {
                if (max == null) {
                    max = u;
                } else if (u.getDigest().getAge() > max.getDigest().getAge()
                        && u.getDigest().getTime() >= max.getDigest().getTime()) {
                    max = u;
                }
            }
            if (max == null) {
                break;
            }
            HashKey removed = new HashKey(max.getDigest().getId());
            state.remove(removed);
            log.trace("removing: {}", removed);
        }
    }

    private boolean put(Message update) {
        AtomicBoolean updated = new AtomicBoolean(false);
        HashKey id = new HashKey(update.getDigest().getId());
        state.compute(id, (k, v) -> {
            if (v == null) {
                // first time, update
                Long current = maxTimes.compute(k, (mid, max) -> Math.max(max == null ? 0 : max,
                                                                          update.getDigest().getTime()));
                if (current - update.getDigest().getTime() > tooOld) {
                    // too old, discard
                    return null;
                }
                updated.set(true);
                return update;
            }
            return Message.newBuilder(v)
                          .setDigest(MessageDigest.newBuilder(v.getDigest())
                                                  .setAge(Math.max(v.getDigest().getAge(),
                                                                   update.getDigest().getAge())))
                          .build();
        });
        return updated.get();
    }

    private void removeOutOfDate() {
        state.entrySet().forEach(entry -> {
            Long max = maxTimes.get(new HashKey(entry.getValue().getDigest().getSource()));
            if (max != null && (max - entry.getValue().getDigest().getTime()) > tooOld) {
                state.remove(entry.getKey());
                log.trace("removing: {}", entry.getKey());
            }
        });
    }
}
