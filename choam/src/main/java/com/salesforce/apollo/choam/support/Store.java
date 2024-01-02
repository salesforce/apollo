/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import static com.salesforce.apollo.choam.support.HashedBlock.height;

/**
 * Kind of a DAO for "nosql" block storage with MVStore from H2
 *
 * @author hal.hildebrand
 */
public class Store {

    private static final String BLOCKS              = "BLOCKS";
    private static final String CERTIFICATIONS      = "CERTIFICATIONS";
    private static final String CHECKPOINT_TEMPLATE = "CHECKPOINT-%s";
    private static final String HASH_TO_HEIGHT      = "HASH_TO_HEIGHT";
    private static final String HASHES              = "HASHES";
    private static final Logger log                 = LoggerFactory.getLogger(Store.class);
    private static final String VIEW_CHAIN          = "VIEW_CHAIN";

    private final MVMap<ULong, byte[]>                   blocks;
    private final MVMap<ULong, byte[]>                   certifications;
    private final TreeMap<ULong, MVMap<Integer, byte[]>> checkpoints = new TreeMap<>();
    private final DigestAlgorithm                        digestAlgorithm;
    private final MVMap<ULong, Digest>                   hashes;
    private final MVMap<Digest, ULong>                   hashToHeight;
    private final MVMap<ULong, ULong>                    viewChain;

    public Store(DigestAlgorithm digestAlgorithm, MVStore store) {
        this.digestAlgorithm = digestAlgorithm;
        hashes = store.openMap(HASHES, new MVMap.Builder<ULong, Digest>().valueType(new DigestType()));
        blocks = store.openMap(BLOCKS);
        hashToHeight = store.openMap(HASH_TO_HEIGHT, new MVMap.Builder<Digest, ULong>().keyType(new DigestType()));
        certifications = store.openMap(CERTIFICATIONS);
        viewChain = store.openMap(VIEW_CHAIN);
    }

    public byte[] block(Digest hash) {
        ULong height = hashToHeight.get(hash);
        return height == null ? null : blocks.get(height);
    }

    public byte[] block(ULong height) {
        return blocks.get(height);
    }

    public Iterator<ULong> blocksFrom(ULong from, ULong to, int max) {
        return new Iterator<>() {
            ULong next;
            int remaining = max;

            {
                next = from;
                while (!blocks.containsKey(next) && remaining > 0 && next.compareTo(to) >= 0) {
                    next = next.subtract(1);
                    remaining--;
                }
                if (next.compareTo(to) < 0 || !blocks.containsKey(next)) {
                    next = null;
                }
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public ULong next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                remaining--;
                ULong returned = next;

                if (next.equals(ULong.valueOf(0))) {
                    next = null;
                } else if (next.compareTo(to) >= 0 && remaining > 0) {
                    next = next.subtract(1);
                    while (!blocks.containsKey(next) && remaining > 0 && next.compareTo(to) >= 0) {
                        next = next.subtract(1);
                        remaining--;
                    }
                    if (next.compareTo(to) < 0 || !blocks.containsKey(next)) {
                        next = null;
                    }
                } else {
                    next = null;
                }
                return returned;
            }
        };
    }

    public List<Certification> certifications(ULong height) {
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

    public boolean completeFrom(ULong from) {
        return lastViewChainFrom(from).equals(ULong.valueOf(0));
    }

    public boolean containsBlock(ULong l) {
        return blocks.containsKey(l);
    }

    public MVMap<Integer, byte[]> createCheckpoint(ULong blockHeight) {
        return blocks.store.openMap(String.format(CHECKPOINT_TEMPLATE, blockHeight));
    }

    public void fetchBlocks(BloomFilter<ULong> blocksBff, Blocks.Builder replication, int max, ULong from, ULong to)
    throws IllegalStateException {
        StreamSupport.stream(((Iterable<ULong>) () -> blocksFrom(from, to, max)).spliterator(), false)
                     .filter(s -> !blocksBff.contains(s))
                     .map(height -> getCertifiedBlock(height))
                     .forEach(block -> replication.addBlocks(block));
    }

    public void fetchViewChain(BloomFilter<ULong> chainBff, Blocks.Builder replication, int maxChainCount,
                               ULong incompleteStart, ULong target) throws IllegalStateException {
        StreamSupport.stream(((Iterable<ULong>) () -> viewChainFrom(incompleteStart, target)).spliterator(), false)
                     .filter(s -> !chainBff.contains(s))
                     .map(height -> getCertifiedBlock(height))
                     .forEach(block -> replication.addBlocks(block));
    }

    public ULong firstGap(ULong from, ULong to) {
        ULong current = from;
        while (current.compareTo(to) > 0) {
            if (blocks.containsKey(current)) {
                current = current.subtract(1);
            } else {
                break;
            }
        }
        return current;
    }

    public void gcFrom(ULong from, ULong to) {
        log.debug("GC'ing Store from: {} to: {}", from, to);
        Iterator<ULong> gcd = blocks.keyIteratorReverse(from.subtract(1));
        if (!gcd.hasNext()) {
            log.trace("Nothing to GC from: {}", from);
            return;
        }
        while (gcd.hasNext()) {
            ULong test = gcd.next();
            if (test.equals(to)) {
                log.trace("Reached last checkpoint: {}", test);
                return;
            }
            delete(test);
        }
    }

    public HashedBlock getBlock(ULong height) {
        byte[] block = block(height);
        try {
            return block == null ? null : new HashedBlock(hash(height), Block.parseFrom(block));
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot deserialize block height: {}", height, e);
            return null;
        }
    }

    public byte[] getBlockBits(ULong height) {
        return blocks.get(height);
    }

    public CertifiedBlock getCertifiedBlock(ULong height) {
        CertifiedBlock.Builder builder = CertifiedBlock.newBuilder();
        HashedBlock block = getBlock(height);
        if (block != null) {
            builder.setBlock(block.block);
        } else {
            return null;
        }
        List<Certification> certs = certifications(height);
        if (certs != null) {
            builder.addAllCertifications(certs);
        }
        return builder.build();
    }

    public HashedCertifiedBlock getLastBlock() {
        ULong lastBlock = blocks.lastKey();
        return lastBlock == null ? null : new HashedCertifiedBlock(digestAlgorithm, getCertifiedBlock(lastBlock));
    }

    public HashedCertifiedBlock getLastView() {
        ULong lastView = checkpoints.lastKey();
        return new HashedCertifiedBlock(digestAlgorithm, getCertifiedBlock(lastView));
    }

    public Digest hash(ULong height) {
        return hashes.get(height);
    }

    public Map<ULong, Digest> hashes() {
        return hashes;
    }

    public ULong lastViewChainFrom(ULong height) {
        ULong last = height;
        ULong next = viewChain.get(height);
        while (next != null && next.compareTo(ULong.valueOf(0)) >= 0) {
            if (!viewChain.containsKey(next)) {
                return last;
            }
            last = next;
            if (next.equals(ULong.valueOf(0))) {
                break;
            }
            next = viewChain.get(next);
        }
        return last;
    }

    public void put(HashedCertifiedBlock cb) {
        transactionally(() -> {
            Certifications certs = Certifications.newBuilder()
                                                 .addAllCerts(cb.certifiedBlock.getCertificationsList())
                                                 .build();
            put(cb.hash, cb.block);
            certifications.put(cb.height(), certs.toByteArray());
        });
    }

    public MVMap<Integer, byte[]> putCheckpoint(ULong blockHeight, File state, Checkpoint checkpoint) {
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
                assert cp.size() == checkpoint.getCount() : "Invalid number of segments: " + cp.size() + " should be: "
                + checkpoint.getCount();
                checkpoints.put(blockHeight, cp);
                return cp;
            });
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    public void rollbackTo(long version) {
        blocks.store.rollbackTo(version);
    }

    public void validate(ULong from, ULong to) throws IllegalStateException {
        AtomicReference<Digest> prevHash = new AtomicReference<>();
        blocks.cursor(to, from, false).forEachRemaining(l -> {
            if (l.equals(to)) {
                Digest k = hash(l);
                if (k == null) {
                    throw new IllegalStateException(String.format("Invalid chain (%s, %s) missing: %s", from, to, l));
                }
                prevHash.set(k);
            } else {
                HashedBlock current = getBlock(l);
                if (current == null) {
                    throw new IllegalStateException(String.format("Invalid chain (%s, %s) missing: %s", from, to, l));
                } else {
                    Digest pointer = new Digest(current.block.getHeader().getPrevious());
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

    public void validateViewChain(ULong from) throws IllegalStateException {
        HashedBlock previous = getBlock(from);
        if (previous == null) {
            throw new IllegalStateException(String.format("Invalid view chain (%s, %s) missing: %s", from, 0, from));
        }
        ULong next = ULong.valueOf(previous.block.getHeader().getLastReconfig());
        HashedBlock current = getBlock(next);
        while (current != null) {
            if (current.height().equals(ULong.valueOf(0))) {
                break;
            }

            Digest pointer = new Digest(previous.block.getHeader().getLastReconfigHash());
            if (pointer.equals(current.hash)) {
                previous = current;
                next = ULong.valueOf(current.block.getHeader().getLastReconfig());
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

    public Iterator<ULong> viewChainFrom(ULong from, ULong to) {
        return new Iterator<>() {
            ULong next;

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
            public ULong next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                ULong returned = next;
                if (next.equals(ULong.valueOf(0))) {
                    next = null;
                } else if (next.compareTo(to) >= 0) {
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

    private void delete(ULong block) {
        if (viewChain.containsKey(block)) {
            log.trace("Retaining reconfiguration: {}", block);
            return;
        }
        transactionally(() -> {
            var bytes = blocks.remove(block);
            if (bytes != null) {
                try {
                    final var b = Block.parseFrom(bytes);
                    log.trace("Deleting block type: {} height: {}", b.getBodyCase(),
                              ULong.valueOf(b.getHeader().getHeight()));
                } catch (InvalidProtocolBufferException e) {
                }
            }
            certifications.remove(block);
            var digest = hashes.remove(block);
            hashToHeight.remove(digest);
        });
    }

    private void put(Digest hash, Block block) {
        ULong height = height(block);
        blocks.put(height, block.toByteArray());
        hashes.put(height, hash);
        hashToHeight.put(hash, height);
        if (block.hasReconfigure() || block.hasGenesis()) {
            viewChain.put(ULong.valueOf(block.getHeader().getHeight()),
                          ULong.valueOf(block.getHeader().getLastReconfig()));
        }
        log.trace("insert: {}:{}", height, hash);
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
