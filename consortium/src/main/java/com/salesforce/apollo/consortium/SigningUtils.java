/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.security.InvalidKeyException;
import java.security.InvalidParameterException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public final class SigningUtils {
    private final static KeyFactory KEY_FACTORY;
    private static final Logger     log = LoggerFactory.getLogger(SigningUtils.class);
    static {
        try {
            KEY_FACTORY = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to get key factory", e);
        }
    }

    public static KeyPair generateKeyPair(final int keySize, String algorithm) {
        try {
            final KeyPairGenerator gen = KeyPairGenerator.getInstance(algorithm);
            gen.initialize(keySize);
            return gen.generateKeyPair();
        } catch (final NoSuchAlgorithmException | InvalidParameterException e) {
            throw new IllegalStateException(e);
        }
    }

    public static PublicKey publicKeyOf(byte[] encoded) {
        if (encoded.length == 0) {
            log.error("Cannot decode public key, zero length encoding");
            return null;
        }
        try {
            return KEY_FACTORY.generatePublic(new X509EncodedKeySpec(encoded));
        } catch (InvalidKeySpecException e) {
            log.error("Cannot decode public key: " + HashKey.bytesToHex(encoded), e);
            return null;
        }
    }

    public static byte[] sign(PrivateKey privateKey, SecureRandom entropy, byte[]... contents) {
        Signature signature = forSigning(privateKey, entropy);
        return sign(signature, contents);
    }

    public static byte[] sign(Signature signature, byte[]... contents) {
        for (byte[] part : contents) {
            try {
                signature.update(part);
            } catch (SignatureException e) {
                log.error("unable to sign contents", e);
                return null;
            }
        }
        try {
            return signature.sign();
        } catch (SignatureException e) {
            log.error("unable to sign contents", e);
            return null;
        }
    }

    public static byte[] sign(Signature signature, SecureRandom entropy, List<byte[]> contents) {
        for (byte[] part : contents) {
            try {
                signature.update(part);
            } catch (SignatureException e) {
                log.error("unable to sign contents", e);
                return null;
            }
        }
        try {
            return signature.sign();
        } catch (SignatureException e) {
            log.error("unable to sign contents", e);
            return null;
        }
    }

    public static Signature signatureForVerification(PublicKey key) {
        Signature signature;
        try {
            signature = Signature.getInstance(Conversion.DEFAULT_SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + Conversion.DEFAULT_SIGNATURE_ALGORITHM, e);
        }
        try {
            signature.initVerify(key);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid public key", e);
        }
        return signature;
    }

    public static boolean validateGenesis(HashKey hash, CertifiedBlock block, Reconfigure initialView,
                                          Context<Member> context, int majority) {
        Map<HashKey, Supplier<Signature>> signatures = new HashMap<>();
        initialView.getViewList().forEach(vm -> {
            HashKey memberID = new HashKey(vm.getId());
            Member member = context.getMember(memberID);
            byte[] encoded = vm.getConsensusKey().toByteArray();
            if (!verify(member.forVerification(Conversion.DEFAULT_SIGNATURE_ALGORITHM), vm.getSignature().toByteArray(),
                        encoded)) {
                log.warn("Could not validate consensus key for {}", memberID);
            }
            PublicKey cKey = publicKeyOf(encoded);
            if (cKey != null) {
                signatures.put(memberID, () -> signatureForVerification(cKey));
            } else {
                log.warn("Could not deserialize consensus key for {}", memberID);
            }
        });
        Function<HashKey, Signature> validators = h -> {
            Supplier<Signature> signature = signatures.get(h);
            return signature == null ? null : signature.get();
        };
        long certifiedCount = block.getCertificationsList()
                                   .parallelStream()
                                   .filter(c -> verify(validators, block.getBlock(), c))
                                   .count();

        log.warn("Certified: {} required: {} provided: {} for genesis: {} on: {}", certifiedCount, majority,
                 signatures.size(), hash);
        return certifiedCount >= majority;
    }

    public static boolean verify(Function<HashKey, Signature> validators, Block block, Certification c) {
        HashKey memberID = new HashKey(c.getId());

        Signature signature = validators.apply(memberID);
        if (signature == null) {
            log.warn("Cannot get signature for verification for: {}", memberID);
            return false;
        }
        boolean verified = verify(signature, c.getSignature().toByteArray(),
                                  Conversion.hashOf(block.getHeader().toByteString()));
        if (!verified) {
            log.warn("Could not verify block using sig from: {}", memberID);
        }
        return verified;
    }

    public static boolean verify(Member member, byte[] signed, byte[]... content) {
        return verify(member.forVerification(Conversion.DEFAULT_SIGNATURE_ALGORITHM), signed, content);
    }

    public static boolean verify(Signature signature, byte[] signed, byte[]... content) {
        try {
            for (byte[] c : content) {
                signature.update(c);
            }
            if (!signature.verify(signed)) {
                return false;
            }
        } catch (Throwable e) {
            log.error("Cannot verify signature", e);
            return false;
        }
        return true;
    }

    private static Signature forSigning(PrivateKey privateKey, SecureRandom entropy) {
        Signature signature;
        try {
            signature = Signature.getInstance(Conversion.DEFAULT_SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + Conversion.DEFAULT_SIGNATURE_ALGORITHM, e);
        }
        try {
            signature.initSign(privateKey, entropy);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid private key", e);
        }
        return signature;
    }

    private SigningUtils() {
    }

}
