/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class CompletableSelectorTest {

    @Test
    public void testIt() throws Exception {
        CompletableFuture.supplyAsync(() -> "hello").whenComplete((i, e) -> {
            System.out.println("Result: " + i);
        });
        
        Thread.sleep(5000);
    }
}
