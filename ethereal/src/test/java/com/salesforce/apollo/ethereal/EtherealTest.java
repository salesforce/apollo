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

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
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
                           crown, data, rsData, false);
    }

    private static class SimpleDataSource implements DataSource {
        final Deque<Any> dataStack = new ArrayDeque<>();

        @Override
        public Any getData() {
            return dataStack.pollFirst();
        }

    }

    @Test
    public void assembled() {
        Controller controller;
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
        try {
            controller.start();
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
        } finally {
            controller.stop();
        }
    }

    @Test
    public void fourWay() throws Exception {
        short nProc = 4;
        SimpleChannel<PreUnit> synchronizer = new SimpleChannel<>(100);

        List<Ethereal> ethereals = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<Orderer> orderers = new ArrayList<>();
        List<Controller> controllers = new ArrayList<>();
        var builder = Config.Builder.empty().addConsensusConfig().setExecutor(ForkJoinPool.commonPool())
                                    .setnProc(nProc);
        Consumer<Orderer> connector = o -> orderers.add(o);
        List<SimpleChannel<PreUnit>> inputChannels = new ArrayList<>();

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            produced.add(new ArrayList<>());
        }

        for (short i = 0; i < nProc; i++) {
            var e = new Ethereal();
            var ds = new SimpleDataSource();
            var out = new SimpleChannel<PreBlock>(100);
            List<PreBlock> output = produced.get(i);
            out.consume(l -> output.addAll(l));
            var controller = e.deterministic(builder.setPid(i).build(), ds, out, synchronizer, connector);
            ethereals.add(e);
            dataSources.add(ds);
            controllers.add(controller);
            SimpleChannel<PreUnit> channel = new SimpleChannel<>(100);
            inputChannels.add(channel);
            for (int d = 0; d < 500; d++) {
                ds.dataStack.add(Any.pack(ByteMessage.newBuilder()
                                                     .setContents(ByteString.copyFromUtf8("pid: " + i + " data: " + d))
                                                     .build()));
            }
        }

        synchronizer.consume(pu -> inputChannels.forEach(e -> pu.forEach(p -> {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e1) {
                return;
            }
            e.submit(p);
        })));
        try {
            controllers.forEach(e -> e.start());
            Utils.waitForCondition(1_000, () -> orderers.size() == nProc);
            for (var o : orderers) {
                assertNotNull(o);
            }

            Deque<Orderer> deque = new ArrayDeque<>(orderers);
            inputChannels.forEach(channel -> {
                var o = deque.remove();
                channel.consume(pus -> o.addPreunits((short) 0, pus));
            });

            Utils.waitForCondition(5_000, 100, () -> {
                for (var pb : produced) {
                    if (pb.size() < 90) {
                        return false;
                    }
                }
                return true;
            });
        } finally {
            controllers.forEach(e -> e.stop());
        }
        for (int i = 0; i < nProc; i++) {
            assertEquals(90, produced.get(i).size(), "Failed to receive all preblocks on process: " + i);
        }
        List<PreBlock> preblocks = produced.get(0);
        List<String> outputOrder = new ArrayList<>();

        for (int i = 1; i < nProc; i++) {
            for (int j = 0; j < preblocks.size(); j++) {
                var a = preblocks.get(j);
                var b = produced.get(i).get(j);
                assertEquals(a.data().size(), b.data().size());
                for (int k = 0; k < a.data().size(); k++) {
                    assertEquals(a.data().get(k), b.data().get(k));
                    outputOrder.add(new String(a.data().get(k).unpack(ByteMessage.class).getContents().toByteArray()));
                }
                assertEquals(a.randomBytes(), b.randomBytes());
            }
        }
    }
}
