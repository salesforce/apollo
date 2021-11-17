/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Checkpoint;
import com.salesfoce.apollo.choam.proto.Header;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

public class HashedBlock implements Comparable<HashedBlock> {
    public static class NullBlock extends HashedBlock {

        public NullBlock(DigestAlgorithm algo) {
            super(algo.getOrigin(), null);
        }

        @Override
        public int compareTo(HashedBlock o) {
            if (this == o) {
                return 0;
            }
            return -1;
        }

        @Override
        public long height() {
            return -1;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(HashedBlock.class);

    public static Header buildHeader(DigestAlgorithm digestAlgorithm, Message body, Digest previous, long height,
                                     long lastCheckpoint, Digest lastCheckpointHash, long lastReconfig,
                                     Digest lastReconfigHash) {
        return Header.newBuilder().setLastCheckpoint(lastCheckpoint)
                     .setLastCheckpointHash(lastCheckpointHash.toDigeste()).setLastReconfig(lastReconfig)
                     .setLastReconfigHash(lastReconfigHash.toDigeste()).setHeight(height)
                     .setPrevious(previous.toDigeste())
                     .setBodyHash(digestAlgorithm.digest(body.toByteString().asReadOnlyByteBufferList()).toDigeste())
                     .build();
    }

    public static Checkpoint checkpoint(DigestAlgorithm algo, File state, int blockSize) {
        Digest stateHash = algo.getOrigin();
        long length = 0;
        if (state != null) {
            try (FileInputStream fis = new FileInputStream(state)) {
                stateHash = algo.digest(fis);
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
            length = state.length();
        }
        Checkpoint.Builder builder = Checkpoint.newBuilder().setByteSize(length).setSegmentSize(blockSize)
                                               .setStateHash(stateHash.toDigeste());
        if (state != null) {
            byte[] buff = new byte[blockSize];
            try (FileInputStream fis = new FileInputStream(state)) {
                for (int read = fis.read(buff); read > 0; read = fis.read(buff)) {
                    ByteString segment = ByteString.copyFrom(buff, 0, read);
                    builder.addSegments(algo.digest(segment).toDigeste());
                }
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
        }
        return builder.build();
    }

    /** Canonical hash of block */
    public static Digest hash(Block block, DigestAlgorithm algo) {
        return algo.digest(block.toByteString().asReadOnlyByteBufferList());
    }

    public static Digest hash(Header header, DigestAlgorithm algo) {
        return algo.digest(header.toByteString());
    }

    public static Digest hashBody(Block block, DigestAlgorithm algo) {
        List<ByteBuffer> buffers = new ArrayList<>();
        switch (block.getBodyCase()) {
        case BODY_NOT_SET:
            break;
        case CHECKPOINT:
            buffers.addAll(block.getCheckpoint().toByteString().asReadOnlyByteBufferList());
            break;
        case EXECUTIONS:
            buffers.addAll(block.getExecutions().toByteString().asReadOnlyByteBufferList());
            break;
        case GENESIS:
            buffers.addAll(block.getGenesis().toByteString().asReadOnlyByteBufferList());
            break;
        case RECONFIGURE:
            buffers.addAll(block.getReconfigure().toByteString().asReadOnlyByteBufferList());
            break;
        default:
            break;

        }
        return algo.digest(buffers);
    }

    public static long height(Block block) {
        return block.getHeader().getHeight();
    }

    public static long height(CertifiedBlock cb) {
        return height(cb.getBlock());
    }

    public final Block  block;
    public final Digest hash;

    public HashedBlock(DigestAlgorithm digestAlgorithm, Block block) {
        this(digestAlgorithm.digest(block.toByteString()), block);
    }

    HashedBlock(Digest hash) {
        this.hash = hash;
        block = null;
    }

    HashedBlock(Digest hash, Block block) {
        this.hash = hash;
        this.block = block;
    }

    @Override
    public int compareTo(HashedBlock o) {
        return hash.equals(o.hash) ? 0 : Long.compare(height(), o.height());
    }

    public Digest getPrevious() {
        return new Digest(block.getHeader().getPrevious());
    }

    public long height() {
        return height(block);
    }
}
