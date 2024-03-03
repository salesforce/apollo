/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import com.salesforce.apollo.context.DynamicContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * @author hal.hildebrand
 */
public class GenesisBootstrapTest extends AbstractLifecycleTest {

    @Test
    public void genesisBootstrap() throws Exception {
        pre();
        System.out.println("Starting late joining node");
        var choam = choams.get(testSubject.getId());
        ((DynamicContext) choam.context()).activate(Collections.singletonList(testSubject));
        choam.start();
        routers.get(testSubject.getId()).start();
        post();
    }

    @Override
    protected int checkpointBlockSize() {
        return 1000;
    }
}
