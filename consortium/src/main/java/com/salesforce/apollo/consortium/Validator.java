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
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Validator {
    private final static KeyFactory KEY_FACTORY;
    private static final Logger     log = LoggerFactory.getLogger(Validator.class);

    static {
        try {
            KEY_FACTORY = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to get key factory", e);
        }
    }

    public static Signature forSigning(PrivateKey privateKey, SecureRandom entropy) {
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

    public static KeyPair generateKeyPair(final int keySize, String algorithm) {
        try {
            final KeyPairGenerator gen = KeyPairGenerator.getInstance(algorithm);
            gen.initialize(keySize);
            return gen.generateKeyPair();
        } catch (final NoSuchAlgorithmException | InvalidParameterException e) {
            throw new IllegalStateException(e);
        }
    }

    public static PublicKey publicKeyOf(byte[] consensusKey) {
        try {
            return KEY_FACTORY.generatePublic(new X509EncodedKeySpec(consensusKey));
        } catch (InvalidKeySpecException e) {
            throw new IllegalStateException("Cannot decode public key", e);
        }
    }

    public static byte[] sign(PrivateKey privateKey, SecureRandom entropy, byte[]... contents) {
        Signature signature = forSigning(privateKey, entropy);
        return sign(signature, entropy, contents);
    }

    public static byte[] sign(Signature signature, SecureRandom entropy, byte[]... contents) {
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

    public static boolean validateGenesis(CertifiedBlock block) {
        Map<HashKey, Supplier<Signature>> signatures = new HashMap<>();
        Reconfigure initialView;
        try {
            initialView = Genesis.parseFrom(block.getBlock().getBody().getContents()).getInitialView();
        } catch (InvalidProtocolBufferException e) {
            log.debug("Error deserializing genesis body", e);
            return false;
        }
        initialView.getViewList().forEach(vm -> {
            HashKey memberID = new HashKey(vm.getId());
            PublicKey cKey = verify(vm.getSignature().toByteArray(), vm.getConsensusKey().toByteArray());
            if (cKey != null) {
                signatures.put(memberID, () -> signatureForVerification(cKey));
            } else {
                log.warn("Could not validate consensus key for {}", memberID);
            }
        });
        Function<HashKey, Signature> validators = h -> {
            Supplier<Signature> signature = signatures.get(h);
            return signature == null ? null : signature.get();
        };
        int toleranceLevel = initialView.getToleranceLevel();
        return block.getCertificationsList()
                    .parallelStream()
                    .filter(c -> verify(validators, block.getBlock(), c))
                    .limit(toleranceLevel + 1)
                    .count() >= toleranceLevel + 1;
    }

    public static PublicKey verify(byte[] signed, byte[] encodedPublicKey) {
        PublicKey cKey = Validator.publicKeyOf(encodedPublicKey);
        if (cKey == null) {
            return null;
        }
        return verify(cKey, signed, encodedPublicKey) ? cKey : null;
    }

    public static boolean verify(Function<HashKey, Signature> validators, Block block, Certification c) {
        HashKey memberID = new HashKey(c.getId());

        Signature signature = validators.apply(memberID);
        return verify(signature, c.getSignature().toByteArray(), block.getHeader().toByteArray());
    }

    public static boolean verify(PublicKey key, byte[] signed, byte[]... content) {
        return verify(signatureForVerification(key), signed, content);

    }

    private static boolean verify(Signature signature, byte[] signed, byte[]... content) {
        try {
            for (byte[] c : content) {
                signature.update(c);
            }
            if (!signature.verify(signed)) {
                return false;
            }
        } catch (Throwable e) {
            log.error("Cannot verify signature", e);
        }
        return true;
    }

    private final Member          leader;
    private final int             toleranceLevel;
    private final Context<Member> view;

    public Validator(Member leader, Context<Member> view, int toleranceLevel) {
        this.leader = leader;
        this.view = view;
        this.toleranceLevel = toleranceLevel;
    }

    public Member getLeader() {
        return leader;
    }

    public int getToleranceLevel() {
        return toleranceLevel;
    }

    public Context<Member> getView() {
        return view;
    }

    public boolean validate(Block block, Validate v, Signature signature) {
        HashKey memberID = new HashKey(v.getId());
        Member member = view.getMember(memberID);
        if (member == null) {
            log.trace("No member found for {}", memberID);
        }
 
        try {
            signature.update(block.getHeader().toByteArray());
        } catch (SignatureException e) {
            log.debug("Error updating validation signature of {}", memberID, e);
            return false;
        }
        try {
            return signature.verify(v.getSignature().toByteArray());
        } catch (SignatureException e) {
            log.debug("Error validating validation signature of {}", memberID, e);
            return false;
        }
    }

    public boolean validate(CertifiedBlock block) {
//        Function<HashKey, Signature> validators = h -> {
//            Member member = view.getMember(h);
//            if (member == null) {
//                return null;
//            }
//            return member.forValidation(Conversion.DEFAULT_SIGNATURE_ALGORITHM);
//        };
//        return block.getCertificationsList()
//                    .parallelStream()
//                    .filter(c -> verify(validators, block.getBlock(), c))
//                    .limit(toleranceLevel + 1)
//                    .count() >= toleranceLevel + 1;
        return false;
    }
}
