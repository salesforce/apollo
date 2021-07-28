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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s.Builder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public interface PreUnit {

    record freeUnit(PreUnit p, Unit[] parents, int level, Map<Short, Unit[]> floor) implements Unit {

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
        public ByteString data() {
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
        public Crown view() {
            return p.view();
        }

        @Override
        public Unit from(Unit[] parents) {
            freeUnit u = new freeUnit(p, parents, Unit.levelFromParents(parents), new HashMap<>());
            u.computeFloor();
            return u;
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
        public Unit[] floor(short pid) {
            var fl = floor.get(pid);
            if (fl != null) {
                return fl;
            }
            if (parents[pid] == null) {
                return new Unit[0];
            }
            return Arrays.copyOfRange(parents, pid, pid + 1);
        }

        private void computeFloor() {
            if (dealing()) {
                return;
            }
            for (short pid = 0; pid < parents.length; pid++) {
                var maximal = Unit.maximalByPid(parents, pid);
                if (maximal.length > 1 || maximal.length == 1 && !maximal[0].equals(parents[pid])) {
                    floor.put(pid, maximal);
                }
            }
        }

        @Override
        public String toString() {
            return "freeUnit[" + creator() + ":" + level() + "(" + height() + ")" + ":" + epoch() + "]";
        }

        @Override
        public String shortString() {
            return p.shortString();
        }

        @Override
        public PreUnit toPreUnit() {
            return p.toPreUnit();
        }

        @Override
        public PreUnit_s toPreUnit_s() {
            return p.toPreUnit_s();
        }
    }

    public record preUnit(short creator, int epoch, int height, Digest hash, Crown crown, ByteString data,
                          byte[] rsData)
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

        public PreUnit_s toPreUnit_s() {
            Builder builder = PreUnit_s.newBuilder().setId(id()).setCrown(crown.toCrown_s());
            if (data != null) {
                builder.setData(data);
            }
            if (rsData != null) {
                builder.setRsData(ByteString.copyFrom(rsData));
            }
            return builder.build();
        }

        @Override
        public Crown view() {
            return crown;
        }

        @Override
        public String toString() {
            return "pu[" + shortString() + "]";
        }

        @Override
        public String shortString() {
            return creator() + ":" + height() + ":" + epoch();
        }

        @Override
        public PreUnit toPreUnit() {
            return this;
        }
    }

    public record DecodedId(int height, short creator, int epoch) {
        public String toString() {
            return "[" + creator + ":" + height + ":" + epoch + "]";
        }
    }

    public static preUnit from(PreUnit_s pus, DigestAlgorithm algo) {
        var decoded = decode(pus.getId());
        byte[] rsData = pus.getRsData().size() > 0 ? pus.getRsData().toByteArray() : null;

        Crown crown = Crown.from(pus.getCrown());
        ByteString data = pus.getData();
        return new preUnit(decoded.creator, decoded.epoch, decoded.height,
                           computeHash(algo, pus.getId(), crown, data, rsData), crown, data, rsData);
    }

    static Digest computeHash(DigestAlgorithm algo, long id, Crown crown, ByteString data, byte[] rsData) {
        var buffers = new ArrayList<ByteBuffer>();
        ByteBuffer idBuff = ByteBuffer.allocate(8);
        idBuff.putLong(id);
        idBuff.flip();

        buffers.add(idBuff);
        if (data != null) {
            buffers.addAll(data.asReadOnlyByteBufferList());
        }
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

    static Unit newFreeUnit(short creator, int epoch, Unit[] parents, int level, ByteString data, byte[] rsBytes,
                            DigestAlgorithm algo) {
        var crown = crownFromParents(parents, algo);
        var height = crown.heights()[creator] + 1;
        var id = id(height, creator, epoch);
        var hash = computeHash(algo, id, crown, data, rsBytes);
        var u = new freeUnit(new preUnit(creator, epoch, height, hash, crown, data, rsBytes), parents, level,
                             new HashMap<>());
        u.computeFloor();
        return u;

    }

    short creator();

    ByteString data();

    default boolean dealing() {
        return height() == 0;
    }

    int epoch();

    default boolean equals(PreUnit v) {
        return creator() == v.creator() && height() == v.height() && epoch() == v.epoch();
    }

    default Unit from(Unit[] parents) {
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

    String shortString();

    PreUnit toPreUnit();

    PreUnit_s toPreUnit_s();

    Crown view();
}
