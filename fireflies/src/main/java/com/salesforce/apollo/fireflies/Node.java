/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.fireflies.AccusationWrapper.forSigning;
import static com.salesforce.apollo.fireflies.NoteWrapper.forSigning;
import static com.salesforce.apollo.fireflies.View.isValidMask;

import java.io.InputStream;
import java.security.Provider;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.fireflies.proto.Accusation;
import com.salesfoce.apollo.fireflies.proto.Accusation.Builder;
import com.salesfoce.apollo.fireflies.proto.AccusationOrBuilder;
import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.NoteOrBuilder;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.BbBackedInputStream;
import com.salesforce.apollo.utils.Utils;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

/**
 * The representation of the "current" member - the subject - of a View.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Node extends Participant implements SigningMember {

    /**
     * Create a mask of length 2t+1 with t randomly disabled rings
     * 
     * @param toleranceLevel - t
     * @return the mask
     */
    public static BitSet createInitialMask(int toleranceLevel, Random entropy) {
        int nbits = 2 * toleranceLevel + 1;
        BitSet mask = new BitSet(nbits);
        List<Boolean> random = new ArrayList<>();
        for (int i = 0; i < toleranceLevel + 1; i++) {
            random.add(true);
        }
        for (int i = 0; i < toleranceLevel; i++) {
            random.add(false);
        }
        Collections.shuffle(random, entropy);
        for (int i = 0; i < nbits; i++) {
            if (random.get(i)) {
                mask.set(i);
            }
        }
        return mask;
    }

    private final FirefliesParameters parameters;
    private final SigningMember       wrapped;

    public Node(SigningMember wrapped, FirefliesParameters p) {
        super(wrapped, p);
        this.wrapped = wrapped;
        this.parameters = p;
    }

    public SslContext forClient(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                                String tlsVersion) {
        return wrapped.forClient(clientAuth, alias, validator, provider, tlsVersion);
    }

    public SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                                String tlsVersion) {
        return wrapped.forServer(clientAuth, alias, validator, provider, tlsVersion);
    }

    /**
     * @return the configuration parameters for this node
     */
    public FirefliesParameters getParameters() {
        return parameters;
    }

    public JohnHancock sign(byte[] message) {
        return wrapped.sign(message);
    }

    @Override
    public JohnHancock sign(InputStream message) {
        return wrapped.sign(message);
    }

    @Override
    public String toString() {
        return "Node[" + getId() + "]";
    }

    public boolean verify(JohnHancock signature, byte[] message) {
        return wrapped.verify(signature, message);
    }

    AccusationWrapper accuse(Participant m, int ringNumber) {
        Builder builder = Accusation.newBuilder();
        Accusation accusation = builder.setEpoch(m.getEpoch())
                                       .setRingNumber(ringNumber)
                                       .setAccuser(qb64(getId()))
                                       .setAccused(qb64(m.getId()))
                                       .setSignature(sign(builder))
                                       .build();
        return new AccusationWrapper(hashAlgorithm.digest(accusation.toByteString()), accusation);
    }

    /**
     * @return a new mask based on the previous mask and previous accusations.
     */
    BitSet nextMask() {
        NoteWrapper current = note;
        if (current == null) {
            BitSet mask = createInitialMask(parameters.toleranceLevel, Utils.secureEntropy());
            assert View.isValidMask(mask, parameters) : "Invalid initial mask: " + mask + "for node: " + getId();
            return mask;
        }

        BitSet mask = new BitSet(parameters.rings);
        mask.flip(0, parameters.rings);
        for (int i : validAccusations.keySet()) {
            if (mask.cardinality() <= parameters.toleranceLevel + 1) {
                assert isValidMask(mask, parameters) : "Invalid mask: " + mask + "for node: " + getId();
                return mask;
            }
            mask.set(i, false);
        }
        if (current.getEpoch() % 2 == 1) {
            BitSet previous = BitSet.valueOf(current.getMask().toByteArray());
            for (int index = 0; index < parameters.rings; index++) {
                if (mask.cardinality() <= parameters.toleranceLevel + 1) {
                    assert View.isValidMask(mask, parameters) : "Invalid mask: " + mask + "for node: " + getId();
                    return mask;
                }
                if (!previous.get(index)) {
                    mask.set(index, false);
                }
            }
        } else {
            // Fill the rest of the mask with randomly set index
            while (mask.cardinality() > parameters.toleranceLevel + 1) {
                int index = Utils.secureEntropy().nextInt(parameters.rings);
                if (mask.get(index)) {
                    mask.set(index, false);
                }
            }
        }
        assert isValidMask(mask, parameters) : "Invalid mask: " + mask + "for node: " + getId();
        return mask;
    }

    /**
     * Generate a new note for the member based on any previous note and previous
     * accusations. The new note has a larger epoch number the the current note.
     */
    void nextNote() {
        NoteWrapper current = note;
        long newEpoch = current == null ? 1 : note.getEpoch() + 1;
        nextNote(newEpoch);
    }

    /**
     * Generate a new note using the new epoch
     * 
     * @param newEpoch
     */
    void nextNote(long newEpoch) {
        Note.Builder builder = Note.newBuilder();
        Note n = builder.setId(qb64(getId()))
                        .setEpoch(newEpoch)
                        .setMask(ByteString.copyFrom(nextMask().toByteArray()))
                        .setSignature(sign(builder))
                        .build();
        note = new NoteWrapper(parameters.hashAlgorithm.digest(n.toByteString()), n);
    }

    private String sign(AccusationOrBuilder builder) {
        return qb64(wrapped.sign(BbBackedInputStream.aggregate(forSigning(builder))));
    }

    private String sign(NoteOrBuilder builder) {
        return qb64(wrapped.sign(BbBackedInputStream.aggregate(forSigning(builder))));
    }
}
