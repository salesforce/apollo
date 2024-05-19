/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.fireflies.proto.Note;
import com.salesforce.apollo.fireflies.proto.Note.Builder;
import com.salesforce.apollo.fireflies.proto.SignedNote;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

import java.util.BitSet;

import static com.salesforce.apollo.cryptography.QualifiedBase64.signature;

/**
 * @author hal.hildebrand
 */
public class NoteWrapper {
    private final Digest     currentView;
    private final Digest     hash;
    private final BitSet     mask;
    private final SignedNote note;

    public NoteWrapper(SignedNote note, DigestAlgorithm algo) {
        assert note != null;
        this.note = note;
        this.hash = JohnHancock.from(note.getSignature()).toDigest(algo);
        this.mask = BitSet.valueOf(note.getNote().getMask().asReadOnlyByteBuffer());
        currentView = Digest.from(note.getNote().getCurrentView());
    }

    public Digest currentView() {
        return currentView;
    }

    public String getEndpoint() {
        return note.getNote().getEndpoint();
    }

    public long getEpoch() {
        return note.getNote().getEpoch();
    }

    public Digest getHash() {
        return hash;
    }

    public Digest getId() {
        return getIdentifier().getDigest();
    }

    public SelfAddressingIdentifier getIdentifier() {
        return (SelfAddressingIdentifier) Identifier.from(note.getNote().getIdentifier());
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

    public Builder newBuilder() {
        return Note.newBuilder(note.getNote());
    }

}
