/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.util.BitSet;

import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public class NoteWrapper {
    private final Digest     hash;
    private final BitSet     mask;
    private final SignedNote note;

    public NoteWrapper(SignedNote note, DigestAlgorithm algo) {
        assert note != null;
        this.note = note;
        this.hash = JohnHancock.from(note.getSignature()).toDigest(algo);
        this.mask = BitSet.valueOf(note.getNote().getMask().asReadOnlyByteBuffer());
    }

    public long getEpoch() {
        return note.getNote().getEpoch();
    }

    public Digest getHash() {
        return hash;
    }

    public Digest getId() {
        return digest(note.getNote().getId());
    }

    public BitSet getMask() {
        return mask;
    }

    public JohnHancock getSignature() {
        return signature(note.getSignature());
    }

    public SignedNote getWrapped() {
        return note;
    }
}
