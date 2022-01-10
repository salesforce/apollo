/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.stereotomy;

import java.sql.Connection;
import java.sql.JDBCType;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.state.Mutator;
import com.salesforce.apollo.state.SqlStateMachine.CallResult;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.db.UniKERL;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;

/**
 * @author hal.hildebrand
 *
 */
public class ShardedKERL extends UniKERL {

    private final Executor                 exec;
    private final Mutator                  mutator;
    private final ScheduledExecutorService scheduler;
    private final Duration                 timeout;

    public ShardedKERL(Connection connection, Mutator mutator, ScheduledExecutorService scheduler, Duration timeout,
                       DigestAlgorithm digestAlgorithm, Executor exec) {
        super(connection, digestAlgorithm);
        this.exec = exec;
        this.mutator = mutator;
        this.scheduler = scheduler;
        this.timeout = timeout;
    }

    @Override
    public CompletableFuture<Void> append(AttachmentEvent event) {
        var returned = new CompletableFuture<Void>();
        returned.complete(null);
        return returned;
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        var call = mutator.call("{ ? = call stereotomy.append(?, ?, ?) }", Collections.singletonList(JDBCType.BINARY),
                                event.getBytes(), event.getIlk(), DigestAlgorithm.DEFAULT.digestCode());
        CompletableFuture<CallResult> submitted;
        try {
            submitted = mutator.execute(exec, call, timeout, scheduler);
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<KeyState>();
            f.completeExceptionally(e);
            return f;
        }
        return submitted.thenApply(callResult -> {
            return (byte[]) callResult.outValues.get(0);
        }).thenApply(b -> {
            try {
                return b == null ? (KeyState) null : new KeyStateImpl(b);
            } catch (InvalidProtocolBufferException e) {
                return null;
            }
        });
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        var batch = mutator.batch();
        for (KeyEvent event : events) {
            batch.execute(mutator.call("{ ? = call stereotomy.append(?, ?, ?) }",
                                       Collections.singletonList(JDBCType.BINARY), event.getBytes(), event.getIlk(),
                                       DigestAlgorithm.DEFAULT.digestCode()));
        }
        try {
            return batch.submit(exec, timeout, scheduler)
                        .thenApply(results -> results.stream()
                                                     .map(result -> (CallResult) result)
                                                     .map(cr -> cr.get(0))
                                                     .map(o -> (byte[]) o)
                                                     .map(b -> {
                                                         try {
                                                             return new KeyStateImpl(keyStateOf(b));
                                                         } catch (InvalidProtocolBufferException e) {
                                                             return (KeyState) null;
                                                         }
                                                     })
                                                     .toList());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<List<KeyState>>();
            f.completeExceptionally(e);
            return f;
        }

    }

    private com.salesfoce.apollo.stereotomy.event.proto.KeyState keyStateOf(byte[] b) throws InvalidProtocolBufferException {
        return com.salesfoce.apollo.stereotomy.event.proto.KeyState.parseFrom(b);
    }
}
