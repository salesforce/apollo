/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.creator;

import static com.salesforce.apollo.ethereal.creator.EpochProofBuilder.decodeShare;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.ethereal.proto.EpochProof;
import com.salesfoce.apollo.ethereal.proto.Proof;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.WeakThresholdKey;

/**
 * proof is a message required to verify if the epoch has finished. It consists
 * of id and hash of the last timing unit of the epoch. This message is signed
 * with a threshold signature.
 * 
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public interface EpochProofBuilder {

    /**
     * @author hal.hildebrand
     *
     */
    public interface Share {
        static Share from(EpochProof proof) {
            // TODO Auto-generated method stub
            return null;
        }

        Short owner();

        JohnHancock toSignature();
    }

    record sharesDB(Config conf, Map<Proof, Map<Short, Share>> data) {
        // Add puts the share that signs msg to the storage. If there are enough shares
        // (for that msg),
        // they are combined and the resulting signature is returned. Otherwise, returns
        // nil.
        JohnHancock add(DecodedShare decoded) {
            var shares = data.get(decoded.proof.getMsg());
            if (shares == null) {
                shares = new HashMap<>();
                data.put(decoded.proof.getMsg(), shares);
            }
            shares.put(decoded.share.owner(), decoded.share);
            if (shares.size() >= conf.WTKey().threshold()) {
                JohnHancock sig = conf.WTKey().combineShares(shares.values());
                if (sig != null) {
                    return sig;
                }
            }
            return null;
        }
    }

    record epochProofImpl(Config conf, int epoch, sharesDB shares) implements EpochProofBuilder {

        // extracts threshold signature shares from finishing units.
        // If there are enough shares to combine, it produces the signature and
        // converts it to core.Data. Otherwise, nil is returned.
        @Override
        public Any tryBuilding(Unit u) {
            // ignore regular units and finishing units with empty data
            if (u.level() < conf.orderStartLevel() + conf.epochLength() || u.data().getSerializedSize() == 0) {
                return null;
            }
            var share = decodeShare(u.data());
            if (share == null) {
                log.warn("Cannot decode share data: {}", u.data());
                return null;
            }
            if (!conf.WTKey().verifyShare(share)) {
                log.warn("Cannot verify share data: {}", u.data());
                return null;
            }
            var sig = shares.add(share);
            if (sig != null) {
                return encodeSignature(sig, share.proof);
            }
            return null;
        }

        private Any encodeSignature(JohnHancock sig, EpochProof proof) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean verify(Unit unit) {
            // TODO Auto-generated method stub
            return false;
        }

        public Any buildShare(Unit lastTimingUnit) {
            var proof = encodeProof(lastTimingUnit);
            Share share = conf.WTKey().createShare(proof);
            if (share != null) {
                return encodeShare(share, proof);
            }
            return Any.getDefaultInstance();
        }

    }

    record DecodedShare(Share share, EpochProof proof) {}

    static final Logger log = LoggerFactory.getLogger(EpochProofBuilder.class);

    /**
     * decodeShare reads signature share and the signed message from Data contained
     * in some unit.
     */
    static DecodedShare decodeShare(Any data) {
        try {
            EpochProof proof = data.unpack(EpochProof.class);
            return new DecodedShare(Share.from(proof), proof);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace(); // for now
            return null;
        }
    }

    /**
     * EpochProofBuilder checks if the given preunit is a proof that a new epoch
     * started.
     */
    static boolean epochProof(PreUnit pu, WeakThresholdKey wtk) {
        if (!pu.dealing() || wtk == null) {
            return false;
        }
        if (pu.epoch() == 0) {
            return true;
        }
        EpochProof decoded;
        try {
            decoded = pu.data().unpack(EpochProof.class);
        } catch (InvalidProtocolBufferException e) {
            // TODO Log
            e.printStackTrace();
            return false;
        }
        int epoch = PreUnit.decode(decoded.getMsg().getEncodedId()).epoch();
        if (epoch + 1 != pu.epoch()) {
            return false;
        }
        return wtk.verifySignature(new JohnHancock(decoded.getSignature()), decoded.getMsg());
    }

    private static Proof encodeProof(Unit lastTimingUnit) {
        return Proof.newBuilder().setEncodedId(lastTimingUnit.id()).setHash(lastTimingUnit.hash().toDigeste()).build();
    }

    /**
     * converts signature share and the signed message into Data that can be put
     * into unit.
     */
    private static Any encodeShare(Share share, Proof proof) {
        return Any.pack(EpochProof.newBuilder().setMsg(proof).setSignature(share.toSignature().toSig()).build());
    }

    Any buildShare(Unit timingUnit);

    Any tryBuilding(Unit unit);

    boolean verify(Unit unit);

}
