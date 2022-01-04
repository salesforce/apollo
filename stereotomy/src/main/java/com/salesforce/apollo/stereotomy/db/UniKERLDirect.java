/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;

import org.jooq.impl.DSL;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public class UniKERLDirect extends UniKERL {

    public UniKERLDirect(Connection connection, DigestAlgorithm digestAlgorithm) {
        super(connection, digestAlgorithm);
    }

    @Override
    public CompletableFuture<Void> append(AttachmentEvent event) {
        dsl.transaction(ctx -> {
            append(DSL.using(ctx), event);
        });
        var result = new CompletableFuture<Void>();
        result.complete(null);
        return result;
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        KeyState newState = processor.process(event);
        dsl.transaction(ctx -> {
            append(DSL.using(ctx), event, newState, digestAlgorithm);
        });
        var f = new CompletableFuture<KeyState>();
        f.complete(newState);
        return f;
    }
}
