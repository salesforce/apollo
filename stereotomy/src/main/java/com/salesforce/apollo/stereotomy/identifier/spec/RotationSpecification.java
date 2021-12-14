/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier.spec;

import static java.util.Objects.requireNonNull;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class RotationSpecification {

    public static class Builder implements Cloneable {
        private EventCoordinates            currentCoords;
        private Digest                      currentDigest;
        private final List<BasicIdentifier> currentWitnesses   = new ArrayList<>();
        private DigestAlgorithm             digestAlgorithm    = DigestAlgorithm.DEFAULT;
        private Format                      format             = Format.PROTOBUF;
        private Identifier                  identifier;
        private final List<PublicKey>       keys               = new ArrayList<>();
        private final List<PublicKey>       nextKeys     = new ArrayList<>();
        private final DigestAlgorithm       nextKeysAlgorithm  = DigestAlgorithm.BLAKE3_256;
        private SigningThreshold            nextSigningThreshold;
        private final List<Seal>            seals              = new ArrayList<>();
        private SignatureAlgorithm          signatureAlgorithm = SignatureAlgorithm.DEFAULT;
        private Map<Integer, Signer>        signers            = new HashMap<>();
        private SigningThreshold            signingThreshold;
        private Version                     version            = Stereotomy.currentVersion();
        private final List<BasicIdentifier> witnesses          = new ArrayList<>();
        private int                         witnessThreshold   = 0;

        public Builder() {
        }

        public Builder addAllSeals(List<Seal> seals) {
            this.seals.addAll(requireNonNull(seals));
            return this;
        }

        public Builder addddWitness(BasicIdentifier prefix) {
            witnesses.add(requireNonNull(prefix));
            return this;
        }

        public Builder addSeal(Seal seal) {
            seals.add(requireNonNull(seal));
            return this;
        }

        public Builder addWitnesses(BasicIdentifier... prefixes) {
            Collections.addAll(witnesses, prefixes);
            return this;
        }

        public Builder addWitnesses(List<BasicIdentifier> prefixes) {
            witnesses.addAll(requireNonNull(prefixes));
            return this;
        }

        public RotationSpecification build() {

            // --- KEYS ---

            if (keys.isEmpty()) {
                throw new IllegalArgumentException("No keys provided.");
            }

            if (signingThreshold == null) {
                signingThreshold = SigningThreshold.unweighted((keys.size() / 2) + 1);
            }

            if (signingThreshold instanceof SigningThreshold.Unweighted) {
                var unw = (SigningThreshold.Unweighted) signingThreshold;
                if (unw.getThreshold() > keys.size()) {
                    throw new IllegalArgumentException("Invalid unweighted signing threshold:" + " keys: " + keys.size()
                    + " threshold: " + unw.getThreshold());
                }
            } else if (signingThreshold instanceof SigningThreshold.Weighted) {
                var w = (SigningThreshold.Weighted) signingThreshold;
                var countOfWeights = Stream.of(w.getWeights()).mapToLong(wts -> wts.length).sum();
                if (countOfWeights != keys.size()) {
                    throw new IllegalArgumentException("Count of weights and count of keys are not equal: " + " keys: "
                    + keys.size() + " weights: " + countOfWeights);
                }
            } else {
                throw new IllegalArgumentException("Unknown SigningThreshold type: " + signingThreshold.getClass());
            }

            // --- NEXT KEYS ---

            // if we don't have it, we use default of majority nextSigningThreshold
            if (nextSigningThreshold == null) {
                nextSigningThreshold = SigningThreshold.unweighted((keys.size() / 2) + 1);
            } else if (nextSigningThreshold instanceof SigningThreshold.Unweighted) {
                var unw = (SigningThreshold.Unweighted) nextSigningThreshold;
                if (unw.getThreshold() > keys.size()) {
                    throw new IllegalArgumentException("Invalid unweighted signing threshold:" + " keys: " + keys.size()
                    + " threshold: " + unw.getThreshold());
                }
            } else if (nextSigningThreshold instanceof SigningThreshold.Weighted) {
                var w = (SigningThreshold.Weighted) nextSigningThreshold;
                var countOfWeights = Stream.of(w.getWeights()).mapToLong(wts -> wts.length).sum();
                if (countOfWeights != keys.size()) {
                    throw new IllegalArgumentException("Count of weights and count of keys are not equal: " + " keys: "
                    + keys.size() + " weights: " + countOfWeights);
                }
            } else {
                throw new IllegalArgumentException("Unknown SigningThreshold type: " + nextSigningThreshold.getClass());
            }

            if (nextKeys.isEmpty()) {
                throw new IllegalArgumentException("None of nextKeys, digestOfNextKeys, or nextKeyConfigurationDigest provided");
            }

            var nextKeyConfigurationDigest = KeyConfigurationDigester.digest(nextSigningThreshold, nextKeys,
                                                                             nextKeysAlgorithm);

            // --- WITNESSES ---
            var added = new ArrayList<>(witnesses);
            added.removeAll(currentWitnesses);

            var removed = new ArrayList<>(currentWitnesses);
            removed.removeAll(witnesses);

            return new RotationSpecification(format, identifier, currentCoords.getSequenceNumber() + 1, currentCoords,
                                             signingThreshold, keys, signers, nextKeyConfigurationDigest,
                                             witnessThreshold, removed, added, seals, version, currentDigest);
        }

        @Override
        public Builder clone() {
            Builder clone;
            try {
                clone = (Builder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException(e);
            }
            return clone;
        }

        public EventCoordinates getCurrentCoords() {
            return currentCoords;
        }

        public Digest getCurrentDigest() {
            return currentDigest;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public Format getFormat() {
            return format;
        }

        public Identifier getIdentifier() {
            return identifier;
        }

        public List<PublicKey> getKeys() {
            return keys;
        }
        
        public Builder setNextKeys(List<PublicKey> nextKeys) {
            this.nextKeys.clear();
            this.nextKeys.addAll(nextKeys);
            return this;
        }

        public List<PublicKey> getNextKeys() {
            return nextKeys;
        }

        public DigestAlgorithm getNextKeysAlgorithm() {
            return nextKeysAlgorithm;
        }

        public SigningThreshold getNextSigningThreshold() {
            return nextSigningThreshold;
        }

        public List<Seal> getSeals() {
            return seals;
        }

        public SignatureAlgorithm getSignatureAlgorithm() {
            return signatureAlgorithm;
        }

        public Map<Integer, Signer> getSigners() {
            return signers;
        }

        public SigningThreshold getSigningThreshold() {
            return signingThreshold;
        }

        public Version getVersion() {
            return version;
        }

        public List<BasicIdentifier> getWitnesses() {
            return witnesses;
        }

        public int getWitnessThreshold() {
            return witnessThreshold;
        }

        public Builder removeWitness(BasicIdentifier identifier) {
            if (!witnesses.remove(requireNonNull(identifier))) {
                throw new IllegalArgumentException("witness not found in witness set");
            }
            return this;
        }

        public Builder removeWitnesses(BasicIdentifier... witnesses) {
            for (var witness : witnesses) {
                removeWitness(witness);
            }
            return this;
        }

        public Builder removeWitnesses(List<BasicIdentifier> witnesses) {
            for (var witness : witnesses) {
                removeWitness(witness);
            }
            return this;
        }

        public Builder setCbor() {
            format = Format.CBOR;
            return this;
        }

        public Builder setCurrentCoords(EventCoordinates currentCoords) {
            this.currentCoords = currentCoords;
            return this;
        }

        public Builder setCurrentDigest(Digest currentDigest) {
            this.currentDigest = currentDigest;
            return this;
        }

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public Builder setIdentifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder setJson() {
            format = Format.JSON;
            return this;
        }

        public Builder setKey(PublicKey publicKey) {
            keys.add(publicKey);
            return this;
        }

        public Builder setKeys(List<PublicKey> publicKeys) {
            keys.addAll(requireNonNull(publicKeys));
            return this;
        }

        public Builder setMessagePack() {
            format = Format.MESSAGE_PACK;
            return this;
        }

        public Builder setNextSigningThreshold(int nextSigningThreshold) {
            if (nextSigningThreshold < 1) {
                throw new IllegalArgumentException("nextSigningThreshold must be 1 or greater");
            }

            this.nextSigningThreshold = SigningThreshold.unweighted(nextSigningThreshold);
            return this;
        }

        public Builder setNextSigningThreshold(SigningThreshold nextSigningThreshold) {
            this.nextSigningThreshold = requireNonNull(nextSigningThreshold);
            return this;
        }

        public Builder setSignatureAlgorithm(SignatureAlgorithm signatureAlgorithm) {
            this.signatureAlgorithm = signatureAlgorithm;
            return this;
        }

        public Builder setSigner(int keyIndex, PrivateKey privateKey) {
            if (keyIndex < 0) {
                throw new IllegalArgumentException("keyIndex must be >= 0");
            }

            signers.put(keyIndex, new SignerImpl(keyIndex, requireNonNull(privateKey)));
            return this;
        }

        public Builder setSigner(Signer signer) {
            requireNonNull(signer);
            if (signer.keyIndex() < 0) {
                throw new IllegalArgumentException("keyIndex must be >= 0");
            }
            signers.put(signer.keyIndex(), signer);
            return this;
        }

        public Builder setSigningThreshold(int signingThreshold) {
            if (signingThreshold < 1) {
                throw new IllegalArgumentException("signingThreshold must be 1 or greater");
            }

            this.signingThreshold = SigningThreshold.unweighted(signingThreshold);
            return this;
        }

        public Builder setSigningThreshold(SigningThreshold signingThreshold) {
            this.signingThreshold = signingThreshold;
            return this;
        }

        public Builder setVersion(Version version) {
            this.version = version;
            return this;
        }

        public Builder setWitnessThreshold(int witnessThreshold) {
            if (witnessThreshold < 0) {
                throw new IllegalArgumentException("witnessThreshold must not be negative");
            }

            this.witnessThreshold = witnessThreshold;
            return this;
        }

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private final List<BasicIdentifier> addedWitnesses;
    private final Format                format;
    private final Identifier            identifier;
    private final List<PublicKey>       keys;
    private final Digest                nextKeys;
    private final EventCoordinates      previous;
    private final Digest                priorEventDigest;
    private final List<BasicIdentifier> removedWitnesses;
    private final List<Seal>            seals;
    private final long                  sequenceNumber;
    private final Map<Integer, Signer>  signers;
    private final SigningThreshold      signingThreshold;
    private final Version               version;
    private final int                   witnessThreshold;

    public RotationSpecification(Format format, Identifier identifier, long sequenceNumber,
                                 EventCoordinates previousEvent, SigningThreshold signingThreshold,
                                 List<PublicKey> keys, Map<Integer, Signer> signers, Digest nextKeys,
                                 int witnessThreshold, List<BasicIdentifier> removedWitnesses,
                                 List<BasicIdentifier> addedWitnesses, List<Seal> seals, Version version,
                                 Digest priorEventDigest) {
        this.format = format;
        this.identifier = identifier;
        this.sequenceNumber = sequenceNumber;
        this.previous = previousEvent;
        this.signingThreshold = signingThreshold;
        this.keys = List.copyOf(keys);
        this.signers = signers;
        this.nextKeys = nextKeys;
        this.witnessThreshold = witnessThreshold;
        this.addedWitnesses = List.copyOf(addedWitnesses);
        this.removedWitnesses = List.copyOf(removedWitnesses);
        this.seals = List.copyOf(seals);
        this.version = version;
        this.priorEventDigest = priorEventDigest;
    }

    public List<BasicIdentifier> getAddedWitnesses() {
        return addedWitnesses;
    }

    public Format getFormat() {
        return format;
    }

    public Identifier getIdentifier() {
        return identifier;
    }

    public List<PublicKey> getKeys() {
        return keys;
    }

    public Digest getNextKeys() {
        return nextKeys;
    }

    public EventCoordinates getPrevious() {
        return previous;
    }

    public Digest getPriorEventDigest() {
        return priorEventDigest;
    }

    public List<BasicIdentifier> getRemovedWitnesses() {
        return removedWitnesses;
    }

    public List<Seal> getSeals() {
        return seals;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public Map<Integer, Signer> getSigners() {
        return signers;
    }

    public SigningThreshold getSigningThreshold() {
        return signingThreshold;
    }

    public Version getVersion() {
        return version;
    }

    public int getWitnessThreshold() {
        return witnessThreshold;
    }

}
