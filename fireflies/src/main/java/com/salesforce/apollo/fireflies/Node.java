/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.View.isValidMask;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.utils.Utils;

/**
 * The representation of the "current" member - the subject - of a View.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Node extends Participant {

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

    /**
     * The node's signing key
     */
    protected final PrivateKey privateKey;

    /**
     * Ye params
     */
    private final FirefliesParameters parameters;

    public Node(CertWithKey identity, FirefliesParameters p) {
        super(identity.getCertificate(), null, p, null);

        privateKey = identity.getPrivateKey();
        this.parameters = p;
    }

    public Signature forSigning() {
        Signature signature;
        try {
            signature = Signature.getInstance(parameters.signatureAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + parameters.signatureAlgorithm, e);
        }
        try {
            signature.initSign(privateKey, Utils.secureEntropy());
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid private key", e);
        }
        return signature;
    }

    public X509Certificate getCA() {
        return parameters.ca;
    }

    /**
     * @return the configuration parameters for this node
     */
    public FirefliesParameters getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return "Node[" + getId() + "]";
    }

    Accusation accuse(Participant m, int ringNumber) {
        return new Accusation(m.getEpoch(), getId(), ringNumber, m.getId(), forSigning());
    }

    Signature forVerification() {
        return forVerification(parameters.signatureAlgorithm);
    }

    /**
     * @return a new mask based on the previous mask and previous accusations.
     */
    BitSet nextMask() {
        Note current = note;
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
            BitSet previous = current.getMask();
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
        Note current = note;
        long newEpoch = current == null ? 1 : note.getEpoch() + 1;
        nextNote(newEpoch);
    }

    /**
     * Generate a new note using the new epoch
     * 
     * @param newEpoch
     */
    void nextNote(long newEpoch) {
        note = new Note(getId(), newEpoch, nextMask(), forSigning());
    }
}
