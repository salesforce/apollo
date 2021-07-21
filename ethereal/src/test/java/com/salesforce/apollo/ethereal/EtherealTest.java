/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.ethereal.Data.PreBlock;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.utils.SimpleChannel;

/**
 * @author hal.hildebrand
 *
 */
public class EtherealTest {

    private Controller controller;

    @AfterEach
    public void disposeController() {
        if (controller != null) {
            controller.stop();
        }
    }

    @Test
    public void assembled() {
        Ethereal e = new Ethereal();
        Config config = Config.newBuilder().build();
        DataSource ds = null;
        SimpleChannel<PreBlock> out = null;
        SimpleChannel<PreUnit> synchronizer = null;
        Consumer<Orderer> connector = null;
        controller = e.deterministic(config, ds, out, synchronizer, connector);
    }
}
