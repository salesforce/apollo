/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public interface Data {

    /**
     * Block is a preblock that has been processed and signed by committee members.
     * It is the final building block of the blockchain produced by the protocol.
     */
    record Block(List<ByteString> data, byte[] randomBytes, long id, List<ByteString> additionalData,
                 JohnHancock signature) {

        public Digest hash(DigestAlgorithm algo) {
            ByteBuffer idBuf = ByteBuffer.allocate(8);
            idBuf.putLong(id);
            idBuf.flip();
            List<ByteBuffer> buffers = new ArrayList<>();
            buffers.add(idBuf);
            data.forEach(e -> buffers.addAll(e.asReadOnlyByteBufferList()));
            buffers.add(ByteBuffer.wrap(randomBytes));
            additionalData.forEach(e -> buffers.addAll(e.asReadOnlyByteBufferList()));
            return algo.digest(buffers);
        }
    }

    record PreBlock(List<ByteString> data, byte[] randomBytes) {
        public Block toBlock(long id, List<ByteString> additionalData) {
            return new Block(data, randomBytes, id, additionalData, null);
        }
    }

    /**
     * return a preblock from a slice of units containing a timing round. It assumes
     * that the timing unit is the last unit in the slice, and that random source
     * data of the timing unit starts with random bytes from the previous level.
     */
    static PreBlock toPreBlock(List<Unit> round) {
        var data = new ArrayList<ByteString>();
        for (Unit u : round) {
            if (!u.dealing()) {// data in dealing units doesn't come from users, these are new epoch proofs
                data.add(u.data());
            }
        }
        var randomBytes = round.get(round.size() - 1).randomSourceData();
        return data.isEmpty() ? null : new PreBlock(data, randomBytes);
    }
}
