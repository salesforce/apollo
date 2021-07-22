/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Data.PreBlock;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import com.salesforce.apollo.utils.SimpleChannel;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class EtherealTest {

    static PreUnit newPreUnit(long id, Crown crown, Any data, byte[] rsData, DigestAlgorithm algo) {
        var t = PreUnit.decode(id);
        if (t.height() != crown.heights()[t.creator()] + 1) {
            throw new IllegalStateException("Inconsistent height information in preUnit id and crown");
        }
        return new preUnit(t.creator(), t.epoch(), t.height(), null, PreUnit.computeHash(algo, id, crown, data, rsData),
                           crown, data, rsData);
    }

    private static class SimpleDataSource implements DataSource {
        @SuppressWarnings("unused")
        final Deque<Any> dataStack = new ArrayDeque<>();

        @Override
        public Any getData() {
            return null;
        }

    }

    private Controller controller;

    @AfterEach
    public void disposeController() {
        if (controller != null) {
            controller.stop();
        }
    }

    @Test
    public void assembled() {
        short nProc = 4;

        AtomicReference<Orderer> ord = new AtomicReference<>();
        var config = Config.Builder.empty().setCanSkipLevel(false).setExecutor(ForkJoinPool.commonPool())
                                   .setnProc(nProc).setNumberOfEpochs(2).build();
        DataSource ds = new SimpleDataSource();
        SimpleChannel<PreBlock> out = new SimpleChannel<>(100);
        SimpleChannel<PreUnit> synchronizer = new SimpleChannel<>(100);
        Consumer<Orderer> connector = a -> ord.set(a);

        List<PreUnit> syncd = new ArrayList<>();
        synchronizer.consumeEach(pu -> syncd.add(pu));

        Ethereal e = new Ethereal();
        controller = e.deterministic(config, ds, out, synchronizer, connector);
        controller.start().run();
        Utils.waitForCondition(1_000, () -> ord.get() != null);
        Orderer orderer = ord.get();
        assertNotNull(orderer);

        for (short pid = 1; pid < config.nProc(); pid++) {
            var crown = Crown.emptyCrown(nProc, DigestAlgorithm.DEFAULT);
            var unitData = Any.getDefaultInstance();
            var rsData = new byte[0];
            long id = PreUnit.id(0, pid, 0);
            var pu = newPreUnit(id, crown, unitData, rsData, DigestAlgorithm.DEFAULT);
            orderer.addPreunits(config.pid(), Collections.singletonList(pu));
        }
        Utils.waitForCondition(2_000, () -> syncd.size() >= 2);
        assertEquals(2, syncd.size());

        PreUnit pu = syncd.get(0);
        assertEquals(0, pu.creator());
        assertEquals(0, pu.epoch());
        assertEquals(0, pu.height());

        pu = syncd.get(1);
        assertEquals(0, pu.creator());
        assertEquals(0, pu.epoch());
        assertEquals(1, pu.height());
    }
}
