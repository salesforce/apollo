/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.nio.ByteBuffer;
import java.util.BitSet;

import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.NoteOrBuilder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public class NoteWrapper {

    public static ByteBuffer forSigning(NoteOrBuilder n) {
        byte[] id = n.getId().toByteArray();
        byte[] mask = n.getMask().toByteArray();
        ByteBuffer note = ByteBuffer.allocate(8 + 4 + id.length + mask.length);
        note.putLong(n.getEpoch()).put(id).put(mask);
        note.flip();
        return note;
    }

    private final Digest hash;
    private final BitSet mask;
    private final Note   note;

    public NoteWrapper(Digest hash, Note note) {
        assert hash != null && note != null;
        this.note = note;
        this.hash = hash;
        this.mask = BitSet.valueOf(note.getMask().asReadOnlyByteBuffer());
    }

    public long getEpoch() {
        return note.getEpoch();
    }

    public Digest getHash() {
        return hash;
    }

    public Digest getId() {
        return digest(note.getId());
    }

    public BitSet getMask() {
        return mask;
    }

    public JohnHancock getSignature() {
        return signature(note.getSignature());
    }

    public Note getWrapped() {
        return note;
    }
}
