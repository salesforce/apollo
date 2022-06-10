/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.joou.ULong;
import org.joou.Unsigned;

import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
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
        public ULong height() {
            return null;
        }
    }

    public static Header buildHeader(DigestAlgorithm digestAlgorithm, Message body, Digest previous, ULong height,
                                     ULong lastCheckpoint, Digest lastCheckpointHash, ULong lastReconfig,
                                     Digest lastReconfigHash) {
        return Header.newBuilder()
                     .setLastCheckpoint(lastCheckpoint == null ? 0 : lastCheckpoint.longValue())
                     .setLastCheckpointHash(lastCheckpointHash.toDigeste())
                     .setLastReconfig(lastReconfig == null ? 0 : lastReconfig.longValue())
                     .setLastReconfigHash(lastReconfigHash.toDigeste())
                     .setHeight(height.longValue())
                     .setPrevious(previous.toDigeste())
                     .setBodyHash(digestAlgorithm.digest(body.toByteString().asReadOnlyByteBufferList()).toDigeste())
                     .build();
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

    public static ULong height(Block block) {
        return Unsigned.ulong(block.getHeader().getHeight());
    }

    public static ULong height(CertifiedBlock cb) {
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
        return hash.equals(o.hash) ? 0 : height().compareTo(o.height());
    }

    public Digest getPrevious() {
        return new Digest(block.getHeader().getPrevious());
    }

    public ULong height() {
        return height(block);
    }

    @Override
    public String toString() {
        return "hb" + hash.toString() + " height: " + height();
    }
}
