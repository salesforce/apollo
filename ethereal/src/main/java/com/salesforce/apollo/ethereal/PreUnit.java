/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.Crown.crownFromParents;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;

/**
 * @author hal.hildebrand
 *
 */
public interface PreUnit {

    record freeUnit(PreUnit p, List<Unit> parents, int level, Map<Short, List<Unit>> floor) implements Unit {

        @Override
        public boolean equals(Object obj) {
            return p.equals(obj);
        }

        @Override
        public int hashCode() {
            return p.hashCode();
        }

        @Override
        public short creator() {
            return p.creator();
        }

        @Override
        public Any data() {
            return p.data();
        }

        @Override
        public int epoch() {
            return p.epoch();
        }

        @Override
        public Digest hash() {
            return p.hash();
        }

        @Override
        public int height() {
            return p.height();
        }

        @Override
        public byte[] randomSourceData() {
            return p.randomSourceData();
        }

        @Override
        public JohnHancock signature() {
            return p.signature();
        }

        @Override
        public Crown view() {
            return p.view();
        }

        @Override
        public boolean aboveWithinProc(Unit v) {
            if (creator() != v.creator()) {
                return false;
            }
            Unit w;
            for (w = this; w != null && w.height() > v.height(); w = w.predecessor())
                ;
            if (w == null) {
                return false;
            }
            return w.hash().equals(v.hash());
        }

        @Override
        public List<Unit> floor(short pid) {
            var fl = floor.get(pid);
            if (fl != null) {
                return fl;
            }
            if (parents.get(pid) == null) {
                return Collections.emptyList();
            }
            return parents.subList(pid, pid + 1);
        }

        private void computeFloor() {
            if (dealing()) {
                return;
            }
            for (short pid = 0; pid < parents.size(); pid++) {
                var maximal = Unit.maximalByPid(parents, pid);
                if (maximal.size() > 1 || maximal.size() == 1 && !maximal.get(0).equals(parents.get(pid))) {
                    floor.put(pid, maximal);
                }
            }
        }

        public String toString() {
            return "freeUnit[" + creator() + ":" + level() + "(" + height() + ")" + ":" + epoch() + "]";
        }
    }

    public record preUnit(short creator, int epoch, int height, JohnHancock signature, Digest hash, Crown crown,
                          Any data, byte[] rsData)
                         implements PreUnit {

        @Override
        public int hashCode() {
            return hash.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Unit u)) {
                return false;
            }
            return hash.equals(u.hash());
        }

        @Override
        public byte[] randomSourceData() {
            return rsData;
        }

        @Override
        public Crown view() {
            return crown;
        }

        public String toString() {
            return "preUnit[" + creator() + ":" + height() + ":" + epoch() + "]";
        }
    }

    public record DecodedId(int height, short creator, int epoch) {}

    static Digest computeHash(DigestAlgorithm algo, long id, Crown crown, Any data, byte[] rsData) {
        var buffers = new ArrayList<ByteBuffer>();
        ByteBuffer idBuff = ByteBuffer.allocate(8);
        idBuff.putLong(id);
        idBuff.flip();

        buffers.add(idBuff);
        buffers.addAll(data.toByteString().asReadOnlyByteBufferList());
        if (rsData != null) {
            buffers.add(ByteBuffer.wrap(rsData));
        }

        for (int h : crown.heights()) {
            ByteBuffer heightBuff = ByteBuffer.allocate(4);
            heightBuff.putInt(h);
            heightBuff.flip();
            buffers.add(heightBuff);
        }
        buffers.add(crown.controlHash().toByteBuffer());

        return algo.digest(buffers);
    }

    static DecodedId decode(long id) {
        var height = (int) (id & ((1 << 16) - 1));
        id >>= 16;
        var creator = (short) (id & ((1 << 16) - 1));
        return new DecodedId(height, creator, (int) (id >> 16));
    }

    static long id(int height, short creator, int epoch) {
        var result = (long) height;
        result += ((long) creator) << 16;
        result += ((long) epoch) << 32;
        return result;
    }

    static Unit newFreeUnit(short creator, int epoch, List<Unit> parents, int level, Any data, byte[] rsBytes,
                            Signer signer, DigestAlgorithm algo) {
        var crown = crownFromParents(parents, algo);
        var height = crown.heights().get(creator) + 1;
        var id = id(height, creator, epoch);
        var hash = computeHash(algo, id, crown, data, rsBytes);
        var signature = signer.sign(hash.toByteBuffer());
        var u = new freeUnit(new preUnit(creator, epoch, height, signature, hash, crown, data, rsBytes), parents, level,
                             new HashMap<>());
        u.computeFloor();
        return u;

    }

    static PreUnit newPreUnit(long id, Crown crown, Any data, byte[] rsData, JohnHancock signature,
                              DigestAlgorithm algo) {
        var t = decode(id);
        if (t.height != crown.heights().get(t.creator) + 1) {
            throw new IllegalStateException("Inconsistent height information in preUnit id and crown");
        }
        return new preUnit(t.creator, t.epoch, t.height, signature, computeHash(algo, id, crown, data, rsData), crown,
                           data, rsData);
    }

    short creator();

    Any data();

    default boolean dealing() {
        return height() == 0;
    }

    int epoch();

    default boolean equals(PreUnit v) {
        return creator() == v.creator() && height() == v.height() && epoch() == v.epoch();
    }

    default Unit from(List<Unit> parents) {
        freeUnit u = new freeUnit(this, parents, Unit.levelFromParents(parents), new HashMap<>());
        u.computeFloor();
        return u;
    }

    Digest hash();

    int height();

    default long id() {
        return id(height(), creator(), epoch());
    }

    default String nickName() {
        return hash().toString();
    }

    byte[] randomSourceData();

    JohnHancock signature();

    Crown view();
}
