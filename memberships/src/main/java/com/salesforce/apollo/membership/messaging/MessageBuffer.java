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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.Message;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.Messages.Builder;
import com.salesfoce.apollo.proto.Push;
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

    public static byte[] sign(HashKey hash, Signature signature) {
        try {
            signature.update(hash.bytes());
            return signature.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign message content", e);
        }
    }

    public static boolean validate(HashKey hash, Message message, Signature signature) {
        try {
            signature.update(hash.bytes());
            return signature.verify(message.getSignature().toByteArray());
        } catch (SignatureException e) {
            log.trace("Message validation error", e);
            return false;
        }
    }

    private static Queue<Entry<HashKey, Message>> findNHighest(Collection<Entry<HashKey, Message>> msgs, int n) {
        Queue<Entry<HashKey, Message>> nthHighest = new PriorityQueue<Entry<HashKey, Message>>(
                (a, b) -> Integer.compare(a.getValue().getAge(), b.getValue().getAge()));

        for (Entry<HashKey, Message> each : msgs) {
            nthHighest.add(each);
            if (nthHighest.size() > n) {
                nthHighest.poll();
            }
        }
        return nthHighest;
    }

    private static HashKey idOf(int sequenceNumber, HashKey from, Any content) {
        ByteBuffer header = ByteBuffer.allocate(32 + 4);
        from.write(header);
        header.putInt(sequenceNumber);
        header.flip();
        List<ByteBuffer> buffers = new ArrayList<>();
        buffers.add(header);
        buffers.addAll(content.toByteString().asReadOnlyByteBufferList());

        return new HashKey(Conversion.hashOf(buffers));
    }

    private static HashKey idOf(Message message) {
        return idOf(message.getSequenceNumber(), new HashKey(message.getSource()), message.getContent());
    }

    private final int                   bufferSize;
    private final AtomicInteger         lastSequenceNumber = new AtomicInteger();
    private final Map<HashKey, Message> state              = new ConcurrentHashMap<>();
    private int                         tooOld;

    public MessageBuffer(int bufferSize, int tooOld) {
        this.bufferSize = bufferSize;
        this.tooOld = tooOld;
    }

    public void clear() {
        state.clear();
    }

    public void gc() {
        log.trace("Compacting buffer");
        purgeTheAged();
        removeOutOfDate();
        log.trace("Buffer free after compact: " + (bufferSize - state.size()));
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
    public List<Message> merge(List<Message> updates, BiPredicate<HashKey, Message> validator) {
        List<Message> merged = updates.parallelStream().filter(message -> {
            HashKey hash = idOf(message);
            if (!validator.test(hash, message)) {
                log.error("Cannot validate message: {}", hash);
                return false;
            }
            return merge(hash, message);
        }).collect(Collectors.toList());
        gc();
        return merged;
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
     * @param msg
     * @param from
     * @param signature
     * 
     * @return the inserted Message
     */
    public Message publish(Any msg, Member from, Signature signature) {
        int sequenceNumber = lastSequenceNumber.getAndIncrement();
        HashKey id = idOf(sequenceNumber, from.getId(), msg);
        Message update = state.computeIfAbsent(id, k -> createUpdate(msg, sequenceNumber, from.getId(),
                                                                     sign(k, signature)));
        gc();
        log.trace("broadcasting: {}:{} on: {}", id, sequenceNumber, from);
        return update;
    }

    public void updatesFor(BloomFilter bff, Push.Builder builder) {
        state.entrySet()
             .stream()
             .peek(entry -> entry.setValue(Message.newBuilder(entry.getValue())
                                                  .setAge(entry.getValue().getAge() + 1)
                                                  .build()))
             .filter(entry -> !bff.contains(entry.getKey()))
             .map(entry -> entry.getValue())
             .forEach(e -> builder.addUpdates(e));
        purgeTheAged();
    }

    private Message createUpdate(Any msg, int sequenceNumber, HashKey from, byte[] signature) {
        return Message.newBuilder()
                      .setSource(from.toID())
                      .setSequenceNumber(sequenceNumber)
                      .setAge(0)
                      .setSignature(ByteString.copyFrom(signature))
                      .setContent(msg)
                      .build();
    }

    private boolean merge(HashKey hash, Message update) {
        if (update.getAge() > tooOld + 1) {
            log.trace("dropped as too old: {}:{}", hash, update.getSequenceNumber());
            return false;
        }
        AtomicBoolean updated = new AtomicBoolean(false);
        state.compute(hash, (k, v) -> {
            if (v == null) {
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
        return updated.get();
    }

    private void purgeTheAged() {
        state.entrySet()
             .stream()
             .filter(e -> e.getValue().getAge() > tooOld)
             .peek(e -> log.trace("removing aged: {}:{}", e.getKey(), e.getValue().getAge()))
             .forEach(e -> state.remove(e.getKey()));
    }

    private void removeOutOfDate() {
        if (state.size() <= bufferSize) {
            return;
        }

        int count = state.size() - bufferSize;
        log.trace("removing overflow count: {}", count);
        findNHighest(state.entrySet(), count).forEach(e -> {
            log.trace("removing overflow: {}:{}", e.getKey(), e.getValue().getAge());
            state.remove(e.getKey());
        });
    }
}
