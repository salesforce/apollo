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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.Message;
import com.salesfoce.apollo.proto.MessageGossip;
import com.salesfoce.apollo.proto.MessageGossip.Builder;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashFunction;
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

    public BloomFilter getBff(int seed, double p) {
        BloomFilter bff = new BloomFilter(new HashFunction(seed, bufferSize, p));
        state.keySet().forEach(h -> bff.add(h));
        return bff;
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

    public MessageGossip process(BloomFilter bff, int seed, double p) {
        Builder builder = MessageGossip.newBuilder();
        state.entrySet().forEach(entry -> {
            if (!bff.contains(entry.getKey())) {
                builder.addUpdates(entry.getValue());
            }
        });
        builder.setBff(getBff(seed, p).toBff());
        MessageGossip gossip = builder.build();
        log.trace("updates: {}", gossip.getUpdatesCount());
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

    public List<Message> updatesFor(BloomFilter bff) {
        return state.entrySet()
                    .stream()
                    .filter(entry -> !bff.contains(entry.getKey()))
                    .map(entry -> entry.getValue())
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
                      .setSource(from.toID())
                      .setId(id.toID())
                      .setAge(0)
                      .setTime(System.currentTimeMillis())
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
                } else if (u.getAge() > max.getAge() && u.getTime() >= max.getTime()) {
                    max = u;
                }
            }
            if (max == null) {
                break;
            }
            HashKey removed = new HashKey(max.getId());
            state.remove(removed);
            log.trace("removing: {}", removed);
        }
    }

    private boolean put(Message update) {
        AtomicBoolean updated = new AtomicBoolean(false);
        HashKey id = new HashKey(update.getId());
        state.compute(id, (k, v) -> {
            if (v == null) {
                // first time, update
                Long current = maxTimes.compute(k, (mid, max) -> Math.max(max == null ? 0 : max, update.getTime()));
                if (current - update.getTime() > tooOld) {
                    // too old, discard
                    return null;
                }
                updated.set(true);
                return update;
            }
            return Message.newBuilder(v).setAge(Math.max(v.getAge(), update.getAge())).build();
        });
        return updated.get();
    }

    private void removeOutOfDate() {
        state.entrySet().forEach(entry -> {
            Long max = maxTimes.get(new HashKey(entry.getValue().getSource()));
            if (max != null && (max - entry.getValue().getTime()) > tooOld) {
                state.remove(entry.getKey());
                log.trace("removing: {}", entry.getKey());
            }
        });
    }
}
