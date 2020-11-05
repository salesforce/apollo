/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.security.SignatureException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.Message;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.Messages.Builder;
import com.salesforce.apollo.membership.Member;
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

    private final AtomicInteger lastSequenceNumber = new AtomicInteger();

    public static ByteBuffer headerBuffer(long ts, int sequenceNumber) {
        ByteBuffer header = ByteBuffer.allocate(8 + 4);
        header.putLong(ts);
        header.putInt(sequenceNumber);
        return header;
    }

    public static byte[] sign(Member from, Signature signature, ByteBuffer header, byte[] bytes) {
        byte[] s;
        try {
            signature.update(from.getId().bytes());
            signature.update(header.array());
            signature.update(bytes);
            s = signature.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign message content", e);
        }
        return s;
    }

    public static boolean validate(Message message, Signature signature) {
        ByteBuffer header = headerBuffer(message.getTime(), message.getSequenceNumber());

        try {
            signature.update(new HashKey(message.getSource()).bytes());
            signature.update(header.array());
            signature.update(message.getContent().toByteArray());
            return signature.verify(message.getSignature().toByteArray());
        } catch (SignatureException e) {
            log.trace("Message validation error", e);
            return false;
        }
    }

    private final int                   bufferSize;
    private final Map<HashKey, Long>    maxTimes = new ConcurrentHashMap<>();
    private final Map<HashKey, Message> state    = new ConcurrentHashMap<>();

    private final int tooOld;

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
        return updates.stream().filter(validator).filter(message -> {
            ByteBuffer header = headerBuffer(message.getTime(), message.getSequenceNumber());
            HashKey hash = new HashKey(Conversion.hashOf(message.getSource().toByteArray(), header.array(),
                                                         message.getContent().toByteArray()));
            return merge(hash, message);
        }).collect(Collectors.toList());
    }

    public Messages process(BloomFilter bff, int seed, double p) {
        Builder builder = Messages.newBuilder();
        state.entrySet().forEach(entry -> {
            if (!bff.contains(entry.getKey())) {
                builder.addUpdates(entry.getValue());
            }
        });
        builder.setBff(getBff(seed, p).toBff());
        Messages gossip = builder.build();
        log.trace("updates: {}", gossip.getUpdatesCount());
        return gossip;
    }

    /**
     * Insert a new message into the buffer from the node
     * 
     * @param ts
     * @param bytes
     * @param from
     * @param signature
     * @return the inserted Message
     */
    public Message publish(long ts, byte[] bytes, Member from, Signature signature) {
        int sequenceNumber = lastSequenceNumber.getAndIncrement();
        ByteBuffer header = headerBuffer(ts, sequenceNumber);
        HashKey id = new HashKey(Conversion.hashOf(from.getId().bytes(), header.array(), bytes));

        byte[] s = sign(from, signature, header, bytes);
        log.trace("broadcasting: {}:{}", id, sequenceNumber);
        Message update = state.computeIfAbsent(id, k -> createUpdate(ts, bytes, sequenceNumber, from.getId(), s));
        gc();
        return update;
    }

    public void updatesFor(BloomFilter bff, com.salesfoce.apollo.proto.Push.Builder builder) {
        state.entrySet()
             .stream()
             .filter(entry -> !bff.contains(entry.getKey()))
             .map(entry -> entry.getValue())
             .forEach(e -> builder.addUpdates(e));
    }

    private void compact() {
        log.trace("Compacting buffer");
        removeOutOfDate();
        purgeTheAged();
    }

    private Message createUpdate(long ts, byte[] content, int sequenceNumber, HashKey from, byte[] signature) {
        return Message.newBuilder()
                      .setSource(from.toID())
                      .setSequenceNumber(sequenceNumber)
                      .setAge(0)
                      .setTime(ts)
                      .setContent(ByteString.copyFrom(content))
                      .setSignature(ByteString.copyFrom(signature))
                      .build();
    }

    private void purgeTheAged() {
        Entry<HashKey, Message> max;
        while (state.size() > bufferSize) {
            max = null;
            for (Entry<HashKey, Message> entry : state.entrySet()) {
                Message u = entry.getValue();
                if (max == null) {
                    max = entry;
                } else if (u.getAge() > max.getValue().getAge() && u.getTime() >= max.getValue().getTime()) {
                    max = entry;
                }
            }
            if (max == null) {
                break;
            }
            HashKey removed = max.getKey();
            state.remove(removed);
            log.trace("removing: {}", removed);
        }
    }

    private boolean merge(HashKey hash, Message update) {
        AtomicBoolean updated = new AtomicBoolean(false);
        state.compute(hash, (k, v) -> {
            if (v == null) {
                // first time, update
                Long current = maxTimes.compute(k, (mid, max) -> Math.max(max == null ? 0 : max, update.getTime()));
                if (current - update.getTime() > tooOld) {
                    log.trace("discarded: {}:{}", hash, update.getSequenceNumber());
                    return null;
                }
                updated.set(true);
                log.trace("added: {}:{}", hash, update.getSequenceNumber());
                return update;
            }
            if (v.getAge() == update.getAge()) {
                return update;
            }
            int age = Math.max(v.getAge(), update.getAge());
            log.trace("merged: {} age: {} prev: {}", hash, age, v.getAge());
            return Message.newBuilder(v).setAge(age).build();
        });
        gc();
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
