/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

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
import com.salesfoce.apollo.messaging.proto.Message;
import com.salesfoce.apollo.messaging.proto.Messages;
import com.salesfoce.apollo.messaging.proto.Push;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.BloomFilter;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MessageBuffer {
    private final static Logger log = LoggerFactory.getLogger(MessageBuffer.class);

    public static byte[] sign(Digest hash, Signature signature) {
        try {
            signature.update(hash.getBytes());
            return signature.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign message content", e);
        }
    }

    static Digest idOf(DigestAlgorithm algorithm, int sequenceNumber, Digest from, Any content) {
        byte[] bytes = qb64(from).getBytes();
        ByteBuffer header = ByteBuffer.allocate(bytes.length + 4);
        header.put(bytes);
        header.putInt(sequenceNumber);
        header.flip();
        List<ByteBuffer> buffers = new ArrayList<>();
        buffers.add(header);
        buffers.addAll(content.toByteString().asReadOnlyByteBufferList());

        return algorithm.digest(buffers);
    }

    private static Queue<Entry<Digest, Message>> findNHighest(Collection<Entry<Digest, Message>> msgs, int n) {
        Queue<Entry<Digest, Message>> nthHighest = new PriorityQueue<Entry<Digest, Message>>(
                (a, b) -> Integer.compare(a.getValue().getAge(), b.getValue().getAge()));

        for (Entry<Digest, Message> each : msgs) {
            nthHighest.add(each);
            if (nthHighest.size() > n) {
                nthHighest.poll();
            }
        }
        return nthHighest;
    }

    private final int                  bufferSize;
    private final DigestAlgorithm      digestAlgorithm;
    private final AtomicInteger        lastSequenceNumber = new AtomicInteger();
    private final Map<Digest, Message> state              = new ConcurrentHashMap<>();
    private final int                  tooOld;

    public MessageBuffer(DigestAlgorithm algorithm, int bufferSize, int tooOld) {
        this.bufferSize = bufferSize;
        this.tooOld = tooOld;
        this.digestAlgorithm = algorithm;
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

    public BloomFilter<Digest> getBff(int seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, bufferSize, p);
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
    public List<Message> merge(List<Message> updates, BiPredicate<Digest, Message> validator) {
        try {
            return updates.parallelStream()
                          .filter(message -> merge(digest(message.getKey()), message, validator))
                          .collect(Collectors.toList());
        } finally {
            gc();
        }
    }

    public Messages process(BloomFilter<Digest> bff, int seed, double p) {
        Messages.Builder builder = Messages.newBuilder();
        state.entrySet().forEach(entry -> {
            if (!bff.contains(entry.getKey())) {
                builder.addUpdates(entry.getValue());
            }
        });
        builder.setBff(getBff(seed, p).toBff().toByteString());
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
    public Message publish(Any msg, SigningMember from) {
        int sequenceNumber = lastSequenceNumber.getAndIncrement();
        Digest id = idOf(digestAlgorithm, sequenceNumber, from.getId(), msg);
        Message update = state.computeIfAbsent(id, k -> createUpdate(msg, sequenceNumber, from.getId(),
                                                                     from.sign(k.toByteString()), k));
        gc();
        log.trace("broadcasting: {}:{} on: {}", id, sequenceNumber, from);
        return update;
    }

    public void updatesFor(BloomFilter<Digest> bff, Push.Builder builder) {
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

    Digest idOf(int sequenceNumber, Digest from, Any content) {
        return idOf(digestAlgorithm, sequenceNumber, from, content);
    }

    private Message createUpdate(Any msg, int sequenceNumber, Digest from, JohnHancock signature, Digest hash) {
        return Message.newBuilder()
                      .setSource(from.toByteString())
                      .setSequenceNumber(sequenceNumber)
                      .setAge(0)
                      .setKey(hash.toByteString())
                      .setSignature(signature.toByteString())
                      .setContent(msg)
                      .build();
    }

    private boolean merge(Digest hash, Message update, BiPredicate<Digest, Message> validator) {
        if (update.getAge() > tooOld + 1) {
            log.trace("dropped as too old: {}:{}", hash, update.getSequenceNumber());
            return false;
        }

        if (!validator.test(hash, update)) {
            return false;
        }
        AtomicBoolean updated = new AtomicBoolean(false);

        state.compute(hash, (k, v) -> {
            if (v == null) {
                updated.set(true);
                log.trace("added: {}:{}", k, update.getSequenceNumber());
                return update;
            }
            if (v.getAge() >= update.getAge()) {
                return v;
            }
            log.trace("merged: {} age: {} prev: {}", k, update.getAge(), v.getAge());
            return Message.newBuilder(v).setAge(update.getAge()).build();
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
