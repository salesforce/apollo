/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.DagTest.collectUnits;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyPair;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.ethereal.proto.ChRbcMessage;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.ethereal.Adder.waitingPreUnit;
import com.salesforce.apollo.ethereal.DagFactory.DagAdder;
import com.salesforce.apollo.utils.Channel;
import com.salesforce.apollo.utils.RoundScheduler;

/**
 * @author hal.hildebrand
 *
 */
public class ChRbcTest {
    @Test
    public void smoke() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/six_units.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());

        short nProc = 4;
        KeyPair keyPair = SignatureAlgorithm.DEFAULT.generateKeyPair();
        var cnf = Config.Builder.empty().setCanSkipLevel(true).setExecutor(ForkJoinPool.commonPool()).setnProc(nProc)
                                .setSigner(new SignerImpl(0, keyPair.getPrivate())).setNumberOfEpochs(2).build();

        RoundScheduler scheduler = new RoundScheduler(10);
        Consumer<ChRbcMessage> bc = m -> {
        };

        Orderer orderer = mock(Orderer.class);
        when(orderer.getConfig()).thenReturn(cnf);
        @SuppressWarnings("unchecked")
        Channel<waitingPreUnit> ready = mock(Channel.class);
        ChRbc chrbc = new ChRbc(scheduler, bc, orderer, ready);
        final short pid = (short) 0;
        var theseUnits = units.get(pid);

        for (List<Unit> u : theseUnits.values()) {
            for (Unit unit : u) {
                chrbc.submit(unit);
            }
        }
    }
}
