/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.creator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Crown;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.creator.Creator.RsData;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CreatorTest {

    static class RandomDataSource implements DataSource {
        final int size;

        RandomDataSource(int size) {
            this.size = size;
        }

        @Override
        public Any getData() {
            byte[] data = new byte[size];
            entropy.nextBytes(data);
            return Any.pack(ByteMessage.newBuilder().setContents(ByteString.copyFrom(data)).build());
        }
    }

    record testEpochProofBuilder(Function<Unit, Boolean> verify) implements EpochProofBuilder {

        @Override
        public Any buildShare(Unit timingUnit) {
            return null;
        }

        @Override
        public Any tryBuilding(Unit unit) {
            return null;
        }

        @Override
        public boolean verify(Unit unit) {
            return verify.apply(unit);
        }
    }

    private final static Random entropy = new Random(0x1638);

    static Creator newCreator(Config cnf, Consumer<Unit> send) {
        var dataSource = new RandomDataSource(10);
        RsData rsData = (i, p, e) -> {
            return null;
        };
        Function<Integer, EpochProofBuilder> epochProofBuilder = epoch -> {
            return new testEpochProofBuilder(u -> true);
        };
        return new Creator(cnf, dataSource, send, rsData, epochProofBuilder);
    }

    static PreUnit newPreUnit(long id, Crown crown, Any data, byte[] rsData, DigestAlgorithm algo) {
        var t = PreUnit.decode(id);
        if (t.height() != crown.heights()[t.creator()] + 1) {
            throw new IllegalStateException("Inconsistent height information in preUnit id and crown");
        }
        return new preUnit(t.creator(), t.epoch(), t.height(), null, PreUnit.computeHash(algo, id, crown, data, rsData),
                           crown, data, rsData);
    }

    @Test
    public void shouldBuildUnitsForEachConsecutiveLevel() throws Exception {
        short nProc = 4;
        KeyPair keyPair = SignatureAlgorithm.DEFAULT.generateKeyPair();
        var cnf = Config.Builder.empty().setCanSkipLevel(false).setExecutor(ForkJoinPool.commonPool()).setnProc(nProc)
                                .setSigner(new Signer(0, keyPair.getPrivate())).setNumberOfEpochs(2).build();
        var unitRec = new ArrayBlockingQueue<Unit>(2);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send);
        assertNotNull(creator);

        var unitBelt = new SubmissionPublisher<Unit>();

        AtomicBoolean finished = new AtomicBoolean();
        var lastTiming = new ArrayBlockingQueue<Unit>(100);
        ForkJoinPool.commonPool().execute(() -> {
            creator.creatUnits(unitBelt, lastTiming);
            finished.set(true);
        });

        Utils.waitForCondition(4_000, 500, () -> finished.get());

        Unit[] parents = new Unit[nProc];
        var maxLevels = 2;

        for (int level = 0; level <= maxLevels; level++) {
            var newParents = new ArrayList<Unit>();
            for (short pid = 1; pid < cnf.nProc(); pid++) {
                var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
                var unitData = Any.getDefaultInstance();
                var rsData = new byte[0];
                long id = PreUnit.id(0, pid, 0);
                var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT);
                var unit = pu.from(parents);
                newParents.add(unit);
                unitBelt.submit(unit);
            }
            parents = newParents.toArray(new Unit[parents.length]);
        }

        assertTrue(finished.get());
        for (int level = 0; level < 4; level++) {
            var createdUnit = unitRec.poll(2, TimeUnit.SECONDS);

            assertNotNull(createdUnit);
            assertEquals(level, createdUnit.level());
            assertEquals(0, createdUnit.creator());
            assertEquals(level, createdUnit.height());
        }

        assertEquals(0, unitBelt.estimateMaximumLag());
        unitBelt.close();
        assertEquals(0, unitRec.size());

    }

    @Test
    public void shouldBuildUnitsOnHighestPossibleLevel() throws Exception {
        short nProc = 4;
        KeyPair keyPair = SignatureAlgorithm.DEFAULT.generateKeyPair();
        var cnf = Config.Builder.empty().setCanSkipLevel(true).setExecutor(ForkJoinPool.commonPool()).setnProc(nProc)
                                .setSigner(new Signer(0, keyPair.getPrivate())).setNumberOfEpochs(2).build();
        var unitRec = new ArrayBlockingQueue<Unit>(2);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send);
        assertNotNull(creator);

        AtomicBoolean finished = new AtomicBoolean();

        var unitBelt = new SubmissionPublisher<Unit>();

        Unit[] parents = new Unit[nProc];
        var maxLevel = 2;

        for (int level = 0; level <= maxLevel; level++) {
            var newParents = new ArrayList<Unit>();
            for (short pid = 1; pid < cnf.nProc(); pid++) {
                var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
                var unitData = Any.getDefaultInstance();
                var rsData = new byte[0];
                long id = PreUnit.id(0, pid, 0);
                var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT);
                var unit = pu.from(parents);
                newParents.add(unit);
                unitBelt.submit(unit);
            }
            parents = newParents.toArray(new Unit[parents.length]);
        }

        var lastTiming = new ArrayBlockingQueue<Unit>(100);

        ForkJoinPool.commonPool().execute(() -> {
            creator.creatUnits(unitBelt, lastTiming);
            finished.set(true);
        });

        Utils.waitForCondition(4_000, 500, () -> finished.get());
        assertTrue(finished.get());

        var createdUnit = unitRec.poll(2, TimeUnit.SECONDS);
        assertNotNull(createdUnit);
        assertEquals(0, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(0, createdUnit.height());

        createdUnit = unitRec.poll(2, TimeUnit.SECONDS);
        assertNotNull(createdUnit);
        assertEquals(maxLevel + 1, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(1, createdUnit.height());

        unitBelt.close();
        assertEquals(0, unitBelt.estimateMaximumLag());
        assertEquals(0, unitRec.size());
    }

    @Test
    public void shouldCreatUnitOnNextLevel() throws Exception {
        short nProc = 4;
        KeyPair keyPair = SignatureAlgorithm.DEFAULT.generateKeyPair();
        var cnf = Config.Builder.empty().setExecutor(ForkJoinPool.commonPool()).setnProc(nProc)
                                .setSigner(new Signer(0, keyPair.getPrivate())).setNumberOfEpochs(2).build();
        var unitRec = new ArrayBlockingQueue<Unit>(2);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send);
        assertNotNull(creator);

        AtomicBoolean finished = new AtomicBoolean();

        var unitBelt = new SubmissionPublisher<Unit>();
        var lastTiming = new ArrayBlockingQueue<Unit>(100);

        ForkJoinPool.commonPool().execute(() -> {
            creator.creatUnits(unitBelt, lastTiming);
            finished.set(true);
        });

        Utils.waitForCondition(4_000, 500, () -> finished.get());
        assertTrue(finished.get());

        Unit[] parents = new Unit[nProc];
        for (short pid = 1; pid < cnf.nProc(); pid++) {
            var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
            var unitData = Any.getDefaultInstance();
            var rsData = new byte[0];
            long id = PreUnit.id(0, pid, 0);
            var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT);
            var unit = pu.from(parents);
            unitBelt.submit(unit);
        }

        var createdUnit = unitRec.poll(2, TimeUnit.SECONDS);
        assertNotNull(createdUnit);
        assertEquals(0, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(0, createdUnit.height());

        createdUnit = unitRec.poll(2, TimeUnit.SECONDS);
        assertNotNull(createdUnit);
        assertEquals(1, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(1, createdUnit.height());

        assertEquals(0, unitBelt.estimateMaximumLag());
        unitBelt.close();
        assertEquals(0, unitRec.size());
    }
}
