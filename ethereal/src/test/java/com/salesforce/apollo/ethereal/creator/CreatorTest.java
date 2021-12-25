/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.creator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Crown;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.creator.Creator.RsData;
import com.salesforce.apollo.utils.SimpleChannel;

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
        public ByteString getData() {
            byte[] data = new byte[size];
            entropy.nextBytes(data);
            return ByteMessage.newBuilder().setContents(ByteString.copyFrom(data)).build().toByteString();
        }
    }

    record testEpochProofBuilder(Function<Unit, Boolean> verify) implements EpochProofBuilder {

        @Override
        public ByteString buildShare(Unit timingUnit) {
            return null;
        }

        @Override
        public ByteString tryBuilding(Unit unit) {
            return null;
        }

        @Override
        public boolean verify(Unit unit) {
            return verify.apply(unit);
        }
    }

    public static final KeyPair DEFAULT_KEYPAIR;

    public static final Signer DEFAULT_SIGNER;

    private final static Random entropy = new Random(0x1638);

    static {
        DEFAULT_KEYPAIR = SignatureAlgorithm.DEFAULT.generateKeyPair();
        DEFAULT_SIGNER = new SignerImpl(DEFAULT_KEYPAIR.getPrivate());
    }

    public static Creator newCreator(Config cnf, Consumer<Unit> send) {
        return newCreator(cnf, send, true);
    }

    public static Creator newCreator(Config cnf, Consumer<Unit> send, boolean proofResult) {
        var dataSource = new RandomDataSource(10);
        RsData rsData = (i, p, e) -> {
            return null;
        };
        Function<Integer, EpochProofBuilder> epochProofBuilder = epoch -> {
            return new testEpochProofBuilder(u -> proofResult);
        };
        final Creator creator = new Creator(cnf, dataSource, send, rsData, epochProofBuilder);
        creator.start();
        return creator;
    }

    public static PreUnit newPreUnit(long id, Crown crown, ByteString data, byte[] rsData, DigestAlgorithm algo,
                                     Signer signer) {
        var t = PreUnit.decode(id);
        if (t.height() != crown.heights()[t.creator()] + 1) {
            throw new IllegalStateException("Inconsistent height information in preUnit id and crown");
        }
        final var salt = new byte[] {};
        final var signature = PreUnit.sign(signer, id, crown, data, rsData, salt);
        return new preUnit(t.creator(), t.epoch(), t.height(), signature.toDigest(algo), crown, data, rsData, signature,
                           salt);
    }

    private double bias = 3.0;

    @Test
    public void invalidFromFutureShouldNotProduceItButKeepProducing() throws Exception {
        short nProc = 4;
        var epoch = 7;
        KeyPair keyPair = SignatureAlgorithm.DEFAULT.generateKeyPair();
        var cnf = Config.Builder.empty().setSigner(DEFAULT_SIGNER).setnProc(nProc)
                                .setSigner(new SignerImpl(keyPair.getPrivate())).setNumberOfEpochs(epoch + 1)
                                .build();

        var unitRec = new ArrayBlockingQueue<Unit>(200);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send);
        assertNotNull(creator);

        var unitBelt = new SimpleChannel<Unit>("Unit belt", 100);
        var lastTiming = new ArrayBlockingQueue<Unit>(2);
        unitBelt.consume(us -> creator.consume(us, lastTiming));

        Unit[] parents = new Unit[nProc];
        var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
        var unitData = ByteString.copyFromUtf8(" ");
        var rsData = new byte[0];
        short pid = 1;
        long id = PreUnit.id(0, pid, epoch);
        var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT, DEFAULT_SIGNER);
        var unit = pu.from(parents, bias);
        assertEquals(epoch, unit.epoch());
        unitBelt.submit(unit);

        var createdUnit = unitRec.poll(2, TimeUnit.SECONDS);
        assertNotNull(createdUnit);
        assertEquals(0, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(0, createdUnit.height());
        assertEquals(0, createdUnit.epoch());

        createdUnit = unitRec.poll(2, TimeUnit.SECONDS);
        assertNotNull(createdUnit);
        assertEquals(0, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(0, createdUnit.height());
        assertEquals(epoch, createdUnit.epoch());

        assertEquals(0, unitBelt.size());
        unitBelt.close();
        assertEquals(0, unitRec.size());
    }

    @Test
    public void shouldBuildUnitsForEachConsecutiveLevel() throws Exception {
        short nProc = 4;
        var cnf = Config.Builder.empty().setCanSkipLevel(false).setnProc(nProc).setSigner(DEFAULT_SIGNER)
                                .setNumberOfEpochs(2).build();
        var unitRec = new ArrayBlockingQueue<Unit>(200);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send);
        assertNotNull(creator);

        var unitBelt = new SimpleChannel<Unit>("Unit belt", 100);

        var lastTiming = new ArrayBlockingQueue<Unit>(100);
        unitBelt.consume(us -> creator.consume(us, lastTiming));

        Unit[] parents = new Unit[nProc];
        var maxLevels = 2;

        for (int level = 0; level <= maxLevels; level++) {
            var newParents = new ArrayList<Unit>();
            for (short pid = 1; pid < cnf.nProc(); pid++) {
                var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
                var unitData = ByteString.copyFromUtf8(" ");
                var rsData = new byte[0];
                long id = PreUnit.id(0, pid, 0);
                var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT, DEFAULT_SIGNER);
                var unit = pu.from(parents, bias);
                newParents.add(unit);
                unitBelt.submit(unit);
            }
            parents = newParents.toArray(new Unit[parents.length]);
        }

        for (int level = 0; level < 4; level++) {
            var createdUnit = unitRec.poll(2, TimeUnit.SECONDS);

            assertNotNull(createdUnit);
            assertEquals(level, createdUnit.level());
            assertEquals(0, createdUnit.creator());
            assertEquals(level, createdUnit.height());
        }

        unitBelt.close();
        assertEquals(0, unitRec.size());

    }

    @Test
    public void shouldBuildUnitsOnHighestPossibleLevel() throws Exception {
        short nProc = 4;
        var cnf = Config.Builder.empty().setCanSkipLevel(true).setnProc(nProc).setSigner(DEFAULT_SIGNER)
                                .setNumberOfEpochs(2).build();
        var unitRec = new ArrayBlockingQueue<Unit>(200);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send);
        assertNotNull(creator);

        var unitBelt = new SimpleChannel<Unit>("Unit belt", 100);

        Unit[] parents = new Unit[nProc];
        var maxLevel = 2;

        for (int level = 0; level <= maxLevel; level++) {
            var newParents = new ArrayList<Unit>();
            for (short pid = 1; pid < cnf.nProc(); pid++) {
                var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
                var unitData = ByteString.copyFromUtf8(" ");
                var rsData = new byte[0];
                long id = PreUnit.id(0, pid, 0);
                var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT, DEFAULT_SIGNER);
                var unit = pu.from(parents, bias);
                newParents.add(unit);
                unitBelt.submit(unit);
            }
            parents = newParents.toArray(new Unit[parents.length]);
        }

        var lastTiming = new ArrayBlockingQueue<Unit>(100);
        unitBelt.consume(us -> creator.consume(us, lastTiming));

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
        assertEquals(0, unitBelt.size());
        assertEquals(0, unitRec.size());
    }

    @Test
    public void shouldCreatUnitOnNextLevel() throws Exception {
        short nProc = 4;
        var cnf = Config.Builder.empty().setnProc(nProc).setSigner(DEFAULT_SIGNER).setNumberOfEpochs(2).build();
        var unitRec = new ArrayBlockingQueue<Unit>(200);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send);
        assertNotNull(creator);

        var unitBelt = new SimpleChannel<Unit>("Unit belt", 100);

        Unit[] parents = new Unit[nProc];
        for (short pid = 1; pid < cnf.nProc(); pid++) {
            var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
            var unitData = ByteString.copyFromUtf8(" ");
            var rsData = new byte[0];
            long id = PreUnit.id(0, pid, 0);
            var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT, DEFAULT_SIGNER);
            var unit = pu.from(parents, bias);
            unitBelt.submit(unit);
        }
        var lastTiming = new ArrayBlockingQueue<Unit>(2);
        unitBelt.consume(us -> creator.consume(us, lastTiming));

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

        assertEquals(0, unitBelt.size());
        unitBelt.close();
        assertEquals(0, unitRec.size());
    }

    @Test
    public void valdFromFutureShouldProduceUnitOfThatEpoch() throws Exception {
        short nProc = 4;
        var epoch = 7;
        var cnf = Config.Builder.empty().setnProc(nProc).setSigner(DEFAULT_SIGNER).setNumberOfEpochs(epoch).build();

        var unitRec = new ArrayBlockingQueue<Unit>(200);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send, false);
        assertNotNull(creator);

        @SuppressWarnings("resource")
        var unitBelt = new SimpleChannel<Unit>("Unit belt", 100);
        var lastTiming = new ArrayBlockingQueue<Unit>(2);
        unitBelt.consume(us -> creator.consume(us, lastTiming));

        Unit[] parents = new Unit[nProc];
        var crown = Crown.newCrownFromParents(parents, DigestAlgorithm.DEFAULT);
        var unitData = ByteString.copyFromUtf8(" ");
        var rsData = new byte[0];
        short pid = 1;
        long id = PreUnit.id(0, pid, epoch);
        var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT, DEFAULT_SIGNER);
        var unit = pu.from(parents, bias);
        assertEquals(epoch, unit.epoch());
        unitBelt.submit(unit);

        var createdUnit = unitRec.poll(2, TimeUnit.SECONDS);
        assertNotNull(createdUnit);
        assertEquals(0, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(0, createdUnit.height());
        assertEquals(0, createdUnit.epoch());

        for (pid = 2; pid < cnf.nProc(); pid++) {
            crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
            unitData = ByteString.copyFromUtf8(" ");
            rsData = new byte[0];
            id = PreUnit.id(0, pid, 0);
            pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT, DEFAULT_SIGNER);
            unit = pu.from(parents, bias);
            unitBelt.submit(unit);
        }

        createdUnit = unitRec.poll(2, TimeUnit.SECONDS);
        assertNotNull(createdUnit);
        assertEquals(1, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(1, createdUnit.height());
        assertEquals(0, createdUnit.epoch());

        assertEquals(0, unitBelt.size());
        unitBelt.close();
        assertEquals(0, unitRec.size());
    }

    @Test
    public void withoutEnoughUnitsOnALevelShouldNotCreateNewUnits() throws Exception {
        short nProc = 4;
        var cnf = Config.Builder.empty().setCanSkipLevel(false).setnProc(nProc).setSigner(DEFAULT_SIGNER)
                                .setNumberOfEpochs(2).build();
        var unitRec = new ArrayBlockingQueue<Unit>(200);
        Consumer<Unit> send = u -> unitRec.add(u);
        var creator = newCreator(cnf, send);
        assertNotNull(creator);

        var unitBelt = new SimpleChannel<Unit>("Unit belt", 100);

        var lastTiming = new ArrayBlockingQueue<Unit>(100);
        unitBelt.consume(us -> creator.consume(us, lastTiming));

        Unit[] parents = new Unit[nProc];

        for (short pid = 3; pid < cnf.nProc(); pid++) {
            var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
            var unitData = ByteString.copyFromUtf8(" ");
            var rsData = new byte[0];
            var id = PreUnit.id(0, pid, 0);
            var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT, DEFAULT_SIGNER);
            var unit = pu.from(parents, bias);
            unitBelt.submit(unit);
        }
        var createdUnit = unitRec.poll(2, TimeUnit.SECONDS);

        assertNotNull(createdUnit);
        assertEquals(0, createdUnit.level());
        assertEquals(0, createdUnit.creator());
        assertEquals(0, createdUnit.height());

        unitBelt.close();
        assertEquals(0, unitRec.size());

    }
}
