/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier.spec;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.salesfoce.apollo.stereotomy.event.proto.IdentifierSpec;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfSigningIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class IdentifierSpecification<D extends Identifier> {

    public static class Builder<D extends Identifier> implements Cloneable {

        public static <I extends Identifier> Builder<I> from(IdentifierSpec parseFrom) {
            return new Builder<I>();
        }

        private final EnumSet<ConfigurationTrait> configurationTraits           = EnumSet.noneOf(ConfigurationTrait.class);
        private Class<? extends Identifier>       derivation                    = SelfAddressingIdentifier.class;
        private DigestAlgorithm                   identifierDigestAlgorithm     = DigestAlgorithm.BLAKE3_256;
        private final List<PublicKey>             keys                          = new ArrayList<>();
        private final List<PublicKey>             nextKeys                      = new ArrayList<>();
        private final DigestAlgorithm             nextKeysAlgorithm             = DigestAlgorithm.BLAKE3_256;
        private SigningThreshold                  nextSigningThreshold;
        private DigestAlgorithm                   selfAddressingDigestAlgorithm = DigestAlgorithm.DEFAULT;
        private SignatureAlgorithm                signatureAlgorithm            = SignatureAlgorithm.DEFAULT;
        private Signer                            signer;
        private SigningThreshold                  signingThreshold;
        private Version                           version                       = Stereotomy.currentVersion();
        private final List<BasicIdentifier>       witnesses                     = new ArrayList<>();

        private int witnessThreshold = 0;

        public Builder<D> addKey(PublicKey key) {
            keys.add(requireNonNull(key));
            return this;
        }

        public Builder<D> basicDerivation(PublicKey key) {
            this.derivation = BasicIdentifier.class;
            this.keys.add(key);
            return this;
        }

        public IdentifierSpecification<D> build() {

            // Keys

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

            // Next keys

            Digest nextKeyConfigurationDigest = null;

            // if we don't have it defined already, we use default of majority
            // nextSigningThreshold
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
                throw new IllegalArgumentException("Next keys not provided");
            }

            nextKeyConfigurationDigest = KeyConfigurationDigester.digest(nextSigningThreshold, nextKeys,
                                                                         nextKeysAlgorithm);

            // Witnesses

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
            return new IdentifierSpecification<D>(derivation, identifierDigestAlgorithm, signingThreshold, keys, signer,
                                                  nextKeyConfigurationDigest, witnessThreshold, witnesses,
                                                  configurationTraits, version, selfAddressingDigestAlgorithm,
                                                  signatureAlgorithm);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Builder<D> clone() {
            Builder<D> clone;
            try {
                clone = (Builder<D>) super.clone();
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

        public DigestAlgorithm getIdentifierDigestAlgorithm() {
            return identifierDigestAlgorithm;
        }

        public List<PublicKey> getKeys() {
            return keys;
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

        public DigestAlgorithm getSelfAddressingDigestAlgorithm() {
            return selfAddressingDigestAlgorithm;
        }

        public SignatureAlgorithm getSignatureAlgorithm() {
            return signatureAlgorithm;
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

        @SuppressWarnings("unchecked")
        public Builder<BasicIdentifier> setBasic() {
            derivation = BasicIdentifier.class;
            return (Builder<BasicIdentifier>) this;
        }

        public Builder<D> setConfigurationTraits(ConfigurationTrait... configurationTraits) {
            Collections.addAll(this.configurationTraits, configurationTraits);
            return this;
        }

        public Builder<D> setDoNotDelegate() {
            configurationTraits.add(ConfigurationTrait.DO_NOT_DELEGATE);
            return this;
        }

        public Builder<D> setEstablishmentEventsOnly() {
            configurationTraits.add(ConfigurationTrait.ESTABLISHMENT_EVENTS_ONLY);
            return this;
        }

        public Builder<D> setIdentifierDigestAlgorithm(DigestAlgorithm algorithm) {
            identifierDigestAlgorithm = algorithm;
            return this;
        }

        public Builder<D> setKeys(List<PublicKey> keys) {
            requireNonNull(keys);

            if (keys.isEmpty()) {
                throw new RuntimeException("Public keys must be provided.");
            }

            this.keys.addAll(keys);
            return this;
        }

        public Builder<D> setNextKeys(List<PublicKey> nextKeys) {
            this.nextKeys.clear();
            this.nextKeys.addAll(nextKeys);
            return this;
        }

        public Builder<D> setNextSigningThreshold(int nextSigningThreshold) {
            if (nextSigningThreshold < 1) {
                throw new IllegalArgumentException("nextSigningThreshold must be 1 or greater");
            }

            this.nextSigningThreshold = SigningThreshold.unweighted(nextSigningThreshold);

            return this;
        }

        public Builder<D> setNextSigningThreshold(SigningThreshold nextSigningThreshold) {
            this.nextSigningThreshold = requireNonNull(nextSigningThreshold);
            return this;
        }

        public Builder<D> setSelfAddressing() {
            derivation = SelfAddressingIdentifier.class;
            return this;
        }

        @SuppressWarnings("unchecked")
        public Builder<SelfAddressingIdentifier> setSelfAddressingDigestAlgorithm(DigestAlgorithm selfAddressingDigestAlgorithm) {
            this.selfAddressingDigestAlgorithm = selfAddressingDigestAlgorithm;
            return (Builder<SelfAddressingIdentifier>) this;
        }

        @SuppressWarnings("unchecked")
        public Builder<SelfSigningIdentifier> setSelfSigning() {
            derivation = SelfSigningIdentifier.class;
            return (Builder<SelfSigningIdentifier>) this;
        }

        public Builder<D> setSignatureAlgorithm(SignatureAlgorithm signatureAlgorithm) {
            this.signatureAlgorithm = signatureAlgorithm;
            return this;
        }

        public Builder<D> setSigner(Signer signer) {
            this.signer = signer;
            return this;
        }

        public Builder<D> setSigningThreshold(int signingThreshold) {
            if (signingThreshold < 1) {
                throw new IllegalArgumentException("signingThreshold must be 1 or greater");
            }

            this.signingThreshold = SigningThreshold.unweighted(signingThreshold);
            return this;
        }

        public Builder<D> setSigningThreshold(SigningThreshold signingThreshold) {
            this.signingThreshold = requireNonNull(signingThreshold);
            return this;
        }

        public Builder<D> setVersion(Version version) {
            this.version = version;
            return this;
        }

        public Builder<D> setWitness(BasicIdentifier witness) {
            witnesses.add(requireNonNull(witness));
            return this;
        }

        public Builder<D> setWitnesses(List<BasicIdentifier> witnesses) {
            witnesses.addAll(requireNonNull(witnesses));
            return this;
        }

        public Builder<D> setWitnessThreshold(int witnessThreshold) {
            if (witnessThreshold < 1) {
                throw new IllegalArgumentException("witnessThreshold must be 1 or greater");
            }

            this.witnessThreshold = witnessThreshold;
            return this;
        }

        public IdentifierSpec toSpec() {
            return IdentifierSpec.newBuilder().build();
        }
    }

    public static BasicIdentifier basic(PublicKey key) {
        return new BasicIdentifier(key);
    }

    public static <D extends Identifier> D identifier(IdentifierSpecification<D> spec, byte[] inceptionStatement) {
        return Identifier.identifier(spec, ByteBuffer.wrap(inceptionStatement));
    }

    public static <D extends Identifier> Builder<D> newBuilder() {
        return new Builder<D>();
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
    private final DigestAlgorithm             identifierDigestAlgorithm;
    private final List<PublicKey>             keys;
    private final Digest                      nextKeys;
    private final DigestAlgorithm             selfAddressingDigestAlgorithm;
    private final SignatureAlgorithm          signatureAlgorithm;
    private final Signer                      signer;
    private final SigningThreshold            signingThreshold;
    private final Version                     version;
    private final List<BasicIdentifier>       witnesses;
    private final int                         witnessThreshold;

    private IdentifierSpecification(Class<? extends Identifier> derivation, DigestAlgorithm identifierDigestAlgorithm,
                                    SigningThreshold signingThreshold, List<PublicKey> keys, Signer signer,
                                    Digest nextKeys, int witnessThreshold, List<BasicIdentifier> witnesses,
                                    Set<ConfigurationTrait> configurationTraits, Version version,
                                    DigestAlgorithm selfAddressingDigestAlgorithm,
                                    SignatureAlgorithm signatureAlgorithm) {
        this.derivation = derivation;
        this.identifierDigestAlgorithm = identifierDigestAlgorithm;
        this.signingThreshold = signingThreshold;
        this.keys = List.copyOf(keys);
        this.signer = signer;
        this.nextKeys = nextKeys;
        this.witnessThreshold = witnessThreshold;
        this.witnesses = List.copyOf(witnesses);
        this.configurationTraits = Set.copyOf(configurationTraits);
        this.version = version;
        this.selfAddressingDigestAlgorithm = selfAddressingDigestAlgorithm;
        this.signatureAlgorithm = signatureAlgorithm;
    }

    public Set<ConfigurationTrait> getConfigurationTraits() {
        return configurationTraits;
    }

    public Class<? extends Identifier> getDerivation() {
        return derivation;
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

    public SignatureAlgorithm getSignatureAlgorithm() {
        return signatureAlgorithm;
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
