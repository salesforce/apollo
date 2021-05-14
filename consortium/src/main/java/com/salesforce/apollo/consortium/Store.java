/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.CollaboratorContext.height;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.Certifications;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock.Builder;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesforce.apollo.consortium.support.HashedBlock;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Pair;

/**
 * Kind of a DAO for "nosql" block storage with MVStore from H2
 * 
 * @author hal.hildebrand
 *
 */
public class Store {
    private static final String BLOCKS              = "BLOCKS";
    private static final String CERTIFICATIONS      = "CERTIFICATIONS";
    private static final String CHECKPOINT_TEMPLATE = "CHECKPOINT-%s";
    private static final String HASH_TO_HEIGHT      = "HASH_TO_HEIGHT";
    private static final String HASHES              = "HASHES";
    private static final Logger log                 = LoggerFactory.getLogger(Store.class);
    private static final String VIEW_CHAIN          = "VIEW_CHAIN";

    private final MVMap<Long, byte[]>               blocks;
    private final MVMap<Long, byte[]>               certifications;
    private final Map<Long, MVMap<Integer, byte[]>> checkpoints = new HashMap<>();
    private final MVMap<Long, byte[]>               hashes;
    private final MVMap<byte[], Long>               hashToHeight;
    private final MVMap<Long, Long>                 viewChain;

    public Store(MVStore store) {
        hashes = store.openMap(HASHES);
        blocks = store.openMap(BLOCKS);
        hashToHeight = store.openMap(HASH_TO_HEIGHT);
        certifications = store.openMap(CERTIFICATIONS);
        viewChain = store.openMap(VIEW_CHAIN);
    }

    public byte[] block(byte[] hash) {
        Long height = hashToHeight.get(hash);
        return height == null ? null : blocks.get(height);
    }

    public byte[] block(long height) {
        return blocks.get(height);
    }

    public Iterator<Long> blocksFrom(long from, long to, int max) {
        return new Iterator<Long>() {
            Long next;
            int  remaining = max;

            {
                next = from;
                while (!blocks.containsKey(next) && remaining > 0 && next >= to) {
                    next--;
                    remaining--;
                }
                if (next < to || !blocks.containsKey(next)) {
                    next = null;
                }
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Long next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                remaining--;
                Long returned = next;

                if (next == 0) {
                    next = null;
                } else if (next >= to && remaining > 0) {
                    next--;
                    while (!blocks.containsKey(next) && remaining > 0 && next >= to) {
                        next--;
                        remaining--;
                    }
                    if (next < to || !blocks.containsKey(next)) {
                        next = null;
                    }
                } else {
                    next = null;
                }
                return returned;
            }
        };
    }

    public List<Certification> certifications(long height) {
        byte[] bs = certifications.get(height);
        if (bs == null) {
            return null;
        }
        try {
            return Certifications.parseFrom(bs).getCertsList();
        } catch (InvalidProtocolBufferException e) {
            log.error("Could not deserialize certifications for {}", height);
            return Collections.emptyList();
        }
    }

    public boolean completeFrom(long from) {
        return lastViewChainFrom(from) == 0;
    }

    public boolean containsBlock(long l) {
        return blocks.containsKey(l);
    }

    public MVMap<Integer, byte[]> createCheckpoint(long blockHeight) {
        return blocks.store.openMap(String.format(CHECKPOINT_TEMPLATE, blockHeight));
    }

    public void fetchBlocks(BloomFilter<Long> blocksBff, Blocks.Builder replication, int max, long from,
                            long to) throws IllegalStateException {
        StreamSupport.stream(((Iterable<Long>) () -> blocksFrom(from, to, max)).spliterator(), false)
                     .filter(s -> !blocksBff.contains(s))
                     .map(height -> getCertifiedBlock(height))
                     .forEach(block -> replication.addBlocks(block));
    }

    public void fetchViewChain(BloomFilter<Long> chainBff, Blocks.Builder replication, int maxChainCount,
                               long incompleteStart, long target) throws IllegalStateException {
        StreamSupport.stream(((Iterable<Long>) () -> viewChainFrom(incompleteStart, target)).spliterator(), false)
                     .filter(s -> !chainBff.contains(s))
                     .map(height -> getCertifiedBlock(height))
                     .forEach(block -> replication.addBlocks(block));
    }

    public long firstGap(long from, long to) {
        long current = from;
        while (current > to) {
            if (blocks.containsKey(current)) {
                current--;
            } else {
                break;
            }
        }
        return current;
    }

    public void gcFrom(long lastCheckpoint) {
        Iterator<Long> gcd = blocks.keyIterator(lastCheckpoint + 1);
        while (gcd.hasNext()) {
            long test = gcd.next();
            if (test != 0 && !isReconfigure(test) && !isCheckpoint(test)) {

            }
        }
    }

    public HashedBlock getBlock(long height) {
        byte[] block = block(height);
        try {
            return block == null ? null : new HashedBlock(new HashKey(hash(height)), Block.parseFrom(block));
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot deserialize block height: {}", height, e);
            return null;
        }
    }

    public byte[] getBlockBits(Long height) {
        return blocks.get(height);
    }

    public CertifiedBlock getCertifiedBlock(long height) {
        Builder builder = CertifiedBlock.newBuilder();
        HashedBlock block = getBlock(height);
        if (block != null) {
            builder.setBlock(block.block);
        }
        List<Certification> certs = certifications(height);
        if (certs != null) {
            builder.addAllCertifications(certs);
        }
        return builder.build();
    }

    public byte[] hash(long height) {
        return hashes.get(height);
    }

    public Map<Long, byte[]> hashes() {
        return hashes;
    }

    public HashKey hashKey(long height) {
        return new HashKey(hash(height));
    }

    public long lastViewChainFrom(long height) {
        long last = height;
        Long next = viewChain.get(height);
        while (next != null && next >= 0) {
            if (!viewChain.containsKey(next)) {
                return last;
            }
            last = next;
            if (next == 0) {
                break;
            }
            next = viewChain.get(next);
        }
        return last;
    }

    public void put(HashKey hash, CertifiedBlock cb) {
        transactionally(() -> {
            Certifications certs = Certifications.newBuilder().addAllCerts(cb.getCertificationsList()).build();
            put(hash, cb.getBlock());
            certifications.put(height(cb.getBlock()), certs.toByteArray());
        });
    }

    public MVMap<Integer, byte[]> putCheckpoint(long blockHeight, File state, Checkpoint checkpoint) {
        try {
            return transactionally(() -> {
                MVMap<Integer, byte[]> cp = checkpoints.get(blockHeight);
                if (cp != null) {
                    return cp;
                }
                cp = createCheckpoint(blockHeight);

                byte[] buffer = new byte[checkpoint.getSegmentSize()];
                try (FileInputStream fis = new FileInputStream(state)) {
                    int i = 0;
                    for (int read = fis.read(buffer); read > 0; read = fis.read(buffer)) {
                        cp.put(i++, Arrays.copyOf(buffer, read));
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Error storing checkpoint " + blockHeight, e);
                }
                assert cp.size() == checkpoint.getSegmentsCount() : "Invalid number of segments: " + cp.size()
                        + " should be: " + checkpoint.getSegmentsCount();
                checkpoints.put(blockHeight, cp);
                return cp;
            });
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    public Pair<HashedCertifiedBlock, HashedCertifiedBlock> restoreFrom() {
        CertifiedBlock genesis = getCertifiedBlock(0L);
        if (genesis == null) {
            return Pair.emptyPair();
        }
        Long lastCheckpoint = checkpoints.keySet().stream().max((a, b) -> a.compareTo(b)).orElse(-1L);
        if (lastCheckpoint < 0) {
            return Pair.emptyPair();
        }
        return new Pair<>(new HashedCertifiedBlock(genesis),
                new HashedCertifiedBlock(getCertifiedBlock(lastCheckpoint)));
    }

    public void rollbackTo(long version) {
        blocks.store.rollbackTo(version);
    }

    public Pair<HashedCertifiedBlock, HashedCertifiedBlock> synchronizeFrom() {
        Long lastViewHeight = viewChain.floorKey(0L);
        if (lastViewHeight != null) {
            Long lastBlockHeight = blocks.floorKey(0L);
            return new Pair<HashedCertifiedBlock, HashedCertifiedBlock>(
                    new HashedCertifiedBlock(getCertifiedBlock(lastViewHeight)),
                    lastBlockHeight != null ? new HashedCertifiedBlock(getCertifiedBlock(lastBlockHeight)) : null);

        }
        return Pair.emptyPair();
    }

    public void validate(long from, long to) throws IllegalStateException {
        AtomicReference<HashKey> prevHash = new AtomicReference<>();
        new Cursor<Long, byte[]>(blocks.getRootPage(), to, from).forEachRemaining(l -> {
            if (l == to) {
                HashKey k = hashKey(l);
                if (k == null) {
                    throw new IllegalStateException(String.format("Invalid chain (%s, %s) missing: %s", from, to, l));
                }
                prevHash.set(k);
            } else {
                HashedBlock current = getBlock(l);
                if (current == null) {
                    throw new IllegalStateException(String.format("Invalid chain (%s, %s) missing: %s", from, to, l));
                } else {
                    HashKey pointer = new HashKey(current.block.getHeader().getPrevious());
                    if (!prevHash.get().equals(pointer)) {
                        throw new IllegalStateException(
                                String.format("Invalid chain (%s, %s) block: %s has invalid previous hash: %s, expected: %s",
                                              from, to, l, pointer, prevHash.get()));
                    } else {
                        prevHash.set(current.hash);
                    }
                }
            }
        });
    }

    public void validateViewChain(long from) throws IllegalStateException {
        HashedBlock previous = getBlock(from);
        if (previous == null) {
            throw new IllegalStateException(String.format("Invalid view chain (%s, %s) missing: %s", from, 0, from));
        }
        long next = previous.block.getHeader().getLastReconfig();
        HashedBlock current = getBlock(next);
        while (current != null) {
            if (current.height() == 0) {
                break;
            }

            HashKey pointer = new HashKey(previous.block.getHeader().getLastReconfigHash());
            if (pointer.equals(current.hash)) {
                previous = current;
                next = current.block.getHeader().getLastReconfig();
                current = getBlock(next);
            } else {
                throw new IllegalStateException(
                        String.format("Invalid view chain (%s, %s) invalid: %s expected: %s have: %s", from, 0,
                                      current.height(), pointer, current.hash));
            }
        }
    }

    public long version() {
        return blocks.store.getStoreVersion();
    }

    public Iterator<Long> viewChainFrom(long from, long to) {
        return new Iterator<Long>() {
            Long next;
            {
                next = viewChain.get(from);
                if (!viewChain.containsKey(next)) {
                    next = null;
                }
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Long next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                Long returned = next;
                if (next == 0) {
                    next = null;
                } else if (next >= to) {
                    next = viewChain.get(next);
                    if (!viewChain.containsKey(next)) {
                        next = null;
                    }
                } else {
                    next = null;
                }
                return returned;
            }
        };
    }

    private boolean isCheckpoint(long test) {
        return checkpoints.containsKey(test);
    }

    private boolean isReconfigure(long next) {
        return viewChain.containsKey(next);
    }

    private void put(HashKey h, Block block) {
        long height = height(block);
        byte[] hash = h.bytes();
        blocks.put(height, block.toByteArray());
        hashes.put(height, hash);
        hashToHeight.put(hash, height);
        BodyType type = block.getBody().getType();
        if (type == BodyType.RECONFIGURE || type == BodyType.GENESIS) {
            viewChain.put(block.getHeader().getHeight(), block.getHeader().getLastReconfig());
        }
        log.trace("insert: {}:{}", height, h);
    }

    private <T> T transactionally(Callable<T> action) throws ExecutionException {
        try {
            T result = action.call();
            blocks.store.commit();
            return result;
        } catch (Throwable t) {
            blocks.store.rollback();
            throw new ExecutionException(t);
        }
    }

    private void transactionally(Runnable action) {
        try {
            action.run();
            blocks.store.commit();
        } catch (Exception t) {
            blocks.store.rollback();
            throw t;
        }
    }
}
