/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.ethereal.proto.EpochProof;
import com.salesfoce.apollo.ethereal.proto.EpochProof.Builder;
import com.salesfoce.apollo.ethereal.proto.Proof;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * the epoch proof is a message required to verify if the epoch has finished. It
 * consists of id and hash of the last timing unit of the epoch. This message is
 * signed with a threshold signature.
 * 
 * @author hal.hildebrand
 *
 */
public interface EpochProofBuilder {

    /**
     * @author hal.hildebrand
     *
     */
    public record Share(short owner, JohnHancock signature) {
        public static Share from(EpochProof proof) {
            try {
                return new Share((short) proof.getOwner(), JohnHancock.from(proof.getSignature()));
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    record sharesDB(Config conf, ConcurrentMap<Digest, ConcurrentMap<Short, Share>> data) {
        /**
         * Add puts the share that signs msg to the storage. If there are enough shares
         * (for that msg), they are combined and the resulting signature is returned.
         * Otherwise, returns nil.
         */
        JohnHancock add(DecodedShare decoded) {
            Digest key = new Digest(decoded.proof.getMsg().getHash());
            var shares = data.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
            log.trace("WTK current: {} threshold: {} on: {}", shares.size(), conf.WTKey().threshold(), conf.logLabel());
            if (decoded.share != null) {
                shares.put(decoded.share.owner(), decoded.share);
                log.trace("share: {} added: {} from: {} on: {}", key, shares.size(), decoded.share.owner(),
                          conf.logLabel());
                if (shares.size() >= conf.WTKey().threshold()) {
                    JohnHancock sig = conf.WTKey().combineShares(shares.values());
                    if (sig != null) {
                        log.trace("WTK threshold reached on: {}", conf.logLabel());
                        return sig;
                    } else {
                        log.trace("WTK threshold reached, but combined shares failed to produce signature on: {}",
                                  conf.logLabel());
                    }
                } else {
                    log.trace("WTK share threshold: {} not reached: {} on: {}", shares.size(), conf.WTKey().threshold(),
                              conf.logLabel());
                }
            } else {
                log.trace("WTK no shares decoded: {} on: {}", shares.size(), conf.logLabel());
            }
            return null;
        }
    }

    public record epochProofImpl(Config conf, int epoch, sharesDB shares) implements EpochProofBuilder {

        /**
         * extracts threshold signature shares from finishing units. If there are enough
         * shares to combine, it produces the signature and converts it to Any.
         * Otherwise, null is returned.
         */
        @Override
        public ByteString tryBuilding(Unit u) {
            // ignore regular units and finishing units with empty data
            if (u.level() <= conf.lastLevel() || (u.data() == null || u.data().isEmpty())) {
                return null;
            }
            var share = decodeShare(u.data());
            if (share == null) {
                log.debug("WTK cannot decode: {} data: {} on: {}", u, u.data(), conf.logLabel());
                return null;
            }
            if (!conf.WTKey().verifyShare(share)) {
                log.warn("WTK cannot verify share: {} data: {} on: {}", u, u.data(), conf.logLabel());
                return null;
            }
            var sig = shares.add(share);
            if (sig != null) {
                log.debug("WTK signature generated: {} on: {}", u, conf.logLabel());
                return encodeSignature(sig, share.proof);
            }
            log.debug("WTK signature generation failed: {} on: {}", u, conf.logLabel());
            return null;
        }

        private ByteString encodeSignature(JohnHancock sig, EpochProof proof) {
            return proof.toByteString();
        }

        @Override
        public boolean verify(Unit unit) {
            if (epoch + 1 != unit.epoch()) {
                return false;
            }
            return epochProof(unit, conf.WTKey());
        }

        @Override
        public ByteString buildShare(Unit lastTimingUnit) {
            var proof = encodeProof(lastTimingUnit);
            Share share = conf.WTKey().createShare(proof, conf.pid());
            log.debug("WTK share built on: {} from: {} proof: {} share: {} on: {}", lastTimingUnit.creator(),
                      lastTimingUnit, proof, share, conf.logLabel());
            if (share != null) {
                return encodeShare(share, proof);
            }
            return ByteString.EMPTY;
        }

    }

    record DecodedShare(Share share, EpochProof proof) {}

    static final Logger log = LoggerFactory.getLogger(EpochProofBuilder.class);

    /**
     * decodeShare reads signature share and the signed message from Data contained
     * in some unit.
     */
    static DecodedShare decodeShare(ByteString data) {
        try {
            EpochProof proof = EpochProof.parseFrom(data);
            return new DecodedShare(Share.from(proof), proof);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    /**
     * EpochProofBuilder checks if the given preunit is a proof that a new epoch
     * started.
     */
    static boolean epochProof(PreUnit pu, WeakThresholdKey wtk) {
        if (!pu.dealing()) {
            return false;
        }
        if (pu.epoch() == 0) {
            return true;
        }
        EpochProof decoded;
        try {
            decoded = EpochProof.parseFrom(pu.data());
        } catch (InvalidProtocolBufferException e) {
            return false;
        }
        int epoch = PreUnit.decode(decoded.getMsg().getEncodedId()).epoch();
        if (epoch + 1 != pu.epoch()) {
            return false;
        }
        return wtk == null ? true : wtk.verifySignature(decoded);
    }

    private static Proof encodeProof(Unit lastTimingUnit) {
        return Proof.newBuilder().setEncodedId(lastTimingUnit.id()).setHash(lastTimingUnit.hash().toDigeste()).build();
    }

    /**
     * converts signature share and the signed message into Data that can be put
     * into unit.
     */
    private static ByteString encodeShare(Share share, Proof proof) {
        Builder builder = EpochProof.newBuilder();
        if (share != null) {
            builder.setOwner(share.owner).setSignature(share.signature().toSig());
        }
        return builder.setMsg(proof).build().toByteString();
    }

    ByteString buildShare(Unit timingUnit);

    ByteString tryBuilding(Unit unit);

    boolean verify(Unit unit);

}
