/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.specification;

import static java.util.Objects.requireNonNull;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfSigningIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class IdentifierSpecification {

    public static class Builder implements Cloneable {

        private final EnumSet<ConfigurationTrait> configurationTraits = EnumSet.noneOf(ConfigurationTrait.class);
        // identifier derivation
        private Class<? extends Identifier> derivation                 = SelfAddressingIdentifier.class;
        private Format                      format                     = Format.PROTOBUF;
        private DigestAlgorithm             identifierDigestAlgorithm  = DigestAlgorithm.BLAKE3_256;
        private final List<PublicKey>       keys                       = new ArrayList<>();
        private final List<Digest>          listOfNextKeyDigests       = new ArrayList<>();
        private final List<PublicKey>       listOfNextKeys             = new ArrayList<>();
        private Digest                      nextKeyConfigurationDigest = Digest.NONE;
        // provide nextKeys + digest algo, nextKeyDigests + digest algo, or
        // nextKeysDigest
        private final DigestAlgorithm nextKeysAlgorithm = DigestAlgorithm.BLAKE3_256;
        // next key configuration
        private SigningThreshold nextSigningThreshold;
        private DigestAlgorithm  selfAddressingDigestAlgorithm = DigestAlgorithm.DEFAULT;
        private Signer           signer;
        // key configuration
        private SigningThreshold            signingThreshold;
        private Version                     version          = Stereotomy.currentVersion();
        private final List<BasicIdentifier> witnesses        = new ArrayList<>();
        private int                         witnessThreshold = 0;

        public Builder basicDerivation(PublicKey key) {
            this.derivation = BasicIdentifier.class;
            this.keys.add(key);
            return this;
        }

        public IdentifierSpecification build() {

            // --- KEYS ---

            if (keys.isEmpty()) {
                throw new RuntimeException("No keys provided.");
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

            if ((!listOfNextKeys.isEmpty() && (nextKeyConfigurationDigest != null))
                    || (!listOfNextKeys.isEmpty() && !listOfNextKeyDigests.isEmpty())
                    || (!listOfNextKeyDigests.isEmpty() && (nextKeyConfigurationDigest != null))) {
                throw new IllegalArgumentException("Only provide one of nextKeys, nextKeyDigests, or a nextKeys.");
            }

            if (nextKeyConfigurationDigest == null) {
                // if we don't have it, we use default of majority nextSigningThreshold
                if (nextSigningThreshold == null) {
                    nextSigningThreshold = SigningThreshold.unweighted((keys.size() / 2) + 1);
                } else if (nextSigningThreshold instanceof SigningThreshold.Unweighted) {
                    var unw = (SigningThreshold.Unweighted) nextSigningThreshold;
                    if (unw.getThreshold() > keys.size()) {
                        throw new IllegalArgumentException("Invalid unweighted signing threshold:" + " keys: "
                                + keys.size() + " threshold: " + unw.getThreshold());
                    }
                } else if (nextSigningThreshold instanceof SigningThreshold.Weighted) {
                    var w = (SigningThreshold.Weighted) nextSigningThreshold;
                    var countOfWeights = Stream.of(w.getWeights()).mapToLong(wts -> wts.length).sum();
                    if (countOfWeights != keys.size()) {
                        throw new IllegalArgumentException("Count of weights and count of keys are not equal: "
                                + " keys: " + keys.size() + " weights: " + countOfWeights);
                    }
                } else {
                    throw new IllegalArgumentException(
                            "Unknown SigningThreshold type: " + nextSigningThreshold.getClass());
                }

                if (listOfNextKeyDigests.isEmpty()) {
                    if (listOfNextKeys.isEmpty()) {
                        throw new IllegalArgumentException(
                                "None of nextKeys, digestOfNextKeys, or nextKeyConfigurationDigest provided");
                    }

                    nextKeyConfigurationDigest = KeyConfigurationDigester.digest(nextSigningThreshold, listOfNextKeys,
                                                                                 nextKeysAlgorithm);
                } else {
                    nextKeyConfigurationDigest = KeyConfigurationDigester.digest(nextSigningThreshold,
                                                                                 listOfNextKeyDigests);
                }
            }

            // --- WITNESSES ---

            if ((witnessThreshold == 0) && !witnesses.isEmpty()) {
                witnessThreshold = (witnesses.size() / 2) + 1;
            }

            if (!witnesses.isEmpty() && ((witnessThreshold < 1) || (witnessThreshold > witnesses.size()))) {
                throw new RuntimeException("Invalid witness threshold:" + " witnesses: " + witnesses.size()
                        + " threshold: " + witnessThreshold);
            }

            // TODO test duplicate detection--need to write equals() hashcode for classes
            if (witnesses.size() != Set.copyOf(witnesses).size()) {
                throw new RuntimeException("List of witnesses has duplicates");
            }

            // validation is provided by spec consumer
            return new IdentifierSpecification(derivation, identifierDigestAlgorithm, format, signingThreshold, keys,
                    signer, nextKeyConfigurationDigest, witnessThreshold, witnesses, configurationTraits, version,
                    selfAddressingDigestAlgorithm);
        }

        public Builder clone() {
            Builder clone;
            try {
                clone = (Builder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException(e);
            }
            return clone;
        }

        public EnumSet<ConfigurationTrait> getConfigurationTraits() {
            return configurationTraits;
        }

        public Class<? extends Identifier> getDerivation() {
            return derivation;
        }

        public Format getFormat() {
            return format;
        }

        public DigestAlgorithm getIdentifierDigestAlgorithm() {
            return identifierDigestAlgorithm;
        }

        public List<PublicKey> getKeys() {
            return keys;
        }

        public List<Digest> getListOfNextKeyDigests() {
            return listOfNextKeyDigests;
        }

        public List<PublicKey> getListOfNextKeys() {
            return listOfNextKeys;
        }

        public Digest getNextKeyConfigurationDigest() {
            return nextKeyConfigurationDigest;
        }

        public DigestAlgorithm getNextKeysAlgorithm() {
            return nextKeysAlgorithm;
        }

        public SigningThreshold getNextSigningThreshold() {
            return nextSigningThreshold;
        }

        public DigestAlgorithm getSelfAddressingDigestAlgorithm() {
            return selfAddressingDigestAlgorithm;
        }

        public Signer getSigner() {
            return signer;
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

        public Builder setConfigurationTraits(ConfigurationTrait... configurationTraits) {
            Collections.addAll(this.configurationTraits, configurationTraits);
            return this;
        }

        public Builder setDoNotDelegate() {
            configurationTraits.add(ConfigurationTrait.DO_NOT_DELEGATE);
            return this;
        }

        public Builder setEstablishmentEventsOnly() {
            configurationTraits.add(ConfigurationTrait.ESTABLISHMENT_EVENTS_ONLY);
            return this;
        }

        public Builder setFormat(Format format) {
            format = requireNonNull(format);
            return this;
        }

        public Builder setIdentifierDigestAlgorithm(DigestAlgorithm algorithm) {
            identifierDigestAlgorithm = algorithm;
            return this;
        }

        public Builder setKey(PublicKey key) {
            keys.add(requireNonNull(key));
            return this;
        }

        public Builder setKeys(List<PublicKey> keys) {
            requireNonNull(keys);

            if (keys.isEmpty()) {
                throw new RuntimeException("Public keys must be provided.");
            }

            keys.addAll(keys);
            return this;
        }

        public Builder setNextKeys(Digest nextKeysDigest) {
            nextKeyConfigurationDigest = requireNonNull(nextKeysDigest);
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
            nextSigningThreshold = requireNonNull(nextSigningThreshold);
            return this;
        }

        public Builder setSelfAddressingDigestAlgorithm(DigestAlgorithm selfAddressingDigestAlgorithm) {
            this.selfAddressingDigestAlgorithm = selfAddressingDigestAlgorithm;
            return this;
        }

        public Builder setSelfSigning() {
            derivation = SelfSigningIdentifier.class;
            return this;
        }

        public Builder setSigner(int keyIndex, PrivateKey privateKey) {
            if (keyIndex < 0) {
                throw new IllegalArgumentException("keyIndex must be >= 0");
            }

            signer = new Signer(keyIndex, requireNonNull(privateKey));
            return this;
        }

        public Builder setSigner(Signer signer) {
            this.signer = signer;
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
            this.signingThreshold = requireNonNull(signingThreshold);
            return this;
        }

        public Builder setVersion(Version version) {
            this.version = version;
            return this;
        }

        public Builder setWitness(BasicIdentifier witness) {
            witnesses.add(requireNonNull(witness));
            return this;
        }

        public Builder setWitnesses(List<BasicIdentifier> witnesses) {
            witnesses.addAll(requireNonNull(witnesses));
            return this;
        }

        public Builder setWitnessThreshold(int witnessThreshold) {
            if (witnessThreshold < 1) {
                throw new IllegalArgumentException("witnessThreshold must be 1 or greater");
            }

            this.witnessThreshold = witnessThreshold;
            return this;
        }

    }

    public static BasicIdentifier basic(PublicKey key) {
        return new BasicIdentifier(key);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Identifier identifier(IdentifierSpecification spec, byte[] inceptionStatement) {
        var derivation = spec.getDerivation();
        if (derivation.isAssignableFrom(BasicIdentifier.class)) {
            return basic(spec.getKeys().get(0));
        } else if (derivation.isAssignableFrom(SelfAddressingIdentifier.class)) {
            return selfAddressing(inceptionStatement, spec.getIdentifierDigestAlgorithm());
        } else if (derivation.isAssignableFrom(SelfSigningIdentifier.class)) {
            return selfSigning(inceptionStatement, spec.getSigner());
        } else {
            throw new IllegalArgumentException("unknown prefix type: " + derivation.getCanonicalName());
        }
    }

    public static SelfAddressingIdentifier selfAddressing(byte[] inceptionStatement, DigestAlgorithm digestAlgorithm) {
        return new SelfAddressingIdentifier(digestAlgorithm.digest(inceptionStatement));
    }

    public static SelfSigningIdentifier selfSigning(byte[] inceptionStatement, Signer signer) {
        var signature = signer.sign(inceptionStatement);
        return new SelfSigningIdentifier(signature);
    }

    private final Set<ConfigurationTrait>     configurationTraits;
    private final Class<? extends Identifier> derivation;
    private final Format                      format;
    private final DigestAlgorithm             identifierDigestAlgorithm;
    private final List<PublicKey>             keys;
    private final Digest                      nextKeys;
    private final DigestAlgorithm             selfAddressingDigestAlgorithm;
    private final Signer                      signer;
    private final SigningThreshold            signingThreshold;
    private final Version                     version;
    private final List<BasicIdentifier>       witnesses;
    private final int                         witnessThreshold;

    private IdentifierSpecification(Class<? extends Identifier> derivation, DigestAlgorithm identifierDigestAlgorithm,
            Format format, SigningThreshold signingThreshold, List<PublicKey> keys, Signer signer, Digest nextKeys,
            int witnessThreshold, List<BasicIdentifier> witnesses, Set<ConfigurationTrait> configurationTraits,
            Version version, DigestAlgorithm selfAddressingDigestAlgorithm) {
        this.derivation = derivation;
        this.identifierDigestAlgorithm = identifierDigestAlgorithm;
        this.format = format;
        this.signingThreshold = signingThreshold;
        this.keys = List.copyOf(keys);
        this.signer = signer;
        this.nextKeys = nextKeys;
        this.witnessThreshold = witnessThreshold;
        this.witnesses = List.copyOf(witnesses);
        this.configurationTraits = Set.copyOf(configurationTraits);
        this.version = version;
        this.selfAddressingDigestAlgorithm = selfAddressingDigestAlgorithm;
    }

    public Set<ConfigurationTrait> getConfigurationTraits() {
        return configurationTraits;
    }

    public Class<? extends Identifier> getDerivation() {
        return derivation;
    }

    public Format getFormat() {
        return format;
    }

    public DigestAlgorithm getIdentifierDigestAlgorithm() {
        return identifierDigestAlgorithm;
    }

    public List<PublicKey> getKeys() {
        return keys;
    }

    public Digest getNextKeys() {
        return nextKeys;
    }

    public DigestAlgorithm getSelfAddressingDigestAlgorithm() {
        return selfAddressingDigestAlgorithm;
    }

    public Signer getSigner() {
        return signer;
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

}
