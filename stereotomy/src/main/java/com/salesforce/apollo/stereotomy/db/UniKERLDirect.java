/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.jooq.impl.DSL;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
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
    public CompletableFuture<KeyState> append(KeyEvent event) {
        KeyState newState = processor.process(event);
        dsl.transaction(ctx -> {
            append(DSL.using(ctx), event, newState, digestAlgorithm);
        });
        var f = new CompletableFuture<KeyState>();
        f.complete(newState);
        return f;
    }

    @Override
    public CompletableFuture<Void> append(List<AttachmentEvent> events) {
        dsl.transaction(ctx -> {
            events.forEach(event -> append(DSL.using(ctx), event));
        });
        var result = new CompletableFuture<Void>();
        result.complete(null);
        return result;
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        List<KeyState> states = new ArrayList<>();
        dsl.transaction(ctx -> {
            var context = DSL.using(ctx);
            events.forEach(event -> {
                KeyState newState = processor.process(event);
                append(context, event, newState, digestAlgorithm);
                states.add(newState);
            });
            attachments.forEach(attach -> append(context, attach));
        });
        var fs = new CompletableFuture<List<KeyState>>();
        fs.complete(states);
        return fs;
    }

    @Override
    public CompletableFuture<Void> appendValidations(EventCoordinates coordinates,
                                                     Map<EventCoordinates, JohnHancock> validations) {
        CompletableFuture<Void> complete = new CompletableFuture<>();

        try {
            dsl.transaction(ctx -> {
                appendValidations(DSL.using(ctx), coordinates, validations);
            });
            complete.complete(null);
        } catch (Exception e) {
            complete.completeExceptionally(e);
        }

        return complete;
    }
}
