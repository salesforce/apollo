/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.stereotomy;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.state.Mutator;
import com.salesforce.apollo.state.SqlStateMachine.CallResult;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.db.UniKERL;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.sql.Connection;
import java.sql.JDBCType;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author hal.hildebrand
 */
public class ShardedKERL extends UniKERL {

    private final Mutator  mutator;
    private final Duration timeout;

    public ShardedKERL(Connection connection, Mutator mutator, Duration timeout, DigestAlgorithm digestAlgorithm) {
        super(connection, digestAlgorithm);
        this.mutator = mutator;
        this.timeout = timeout;
    }

    @Override
    public KeyState append(KeyEvent event) {
        var call = mutator.call("{ ? = call stereotomy.append(?, ?, ?) }", Collections.singletonList(JDBCType.BINARY),
                                event.getBytes(), event.getIlk(), DigestAlgorithm.DEFAULT.digestCode());
        CompletableFuture<CallResult> submitted;
        try {
            submitted = mutator.execute(call, timeout);
        } catch (InvalidTransaction e) {
            throw new IllegalStateException(e);
        }
        var b = submitted.thenApply(callResult -> {
            return (byte[]) callResult.outValues.get(0);
        });
        try {
            return b == null ? (KeyState) null : new KeyStateImpl(b.get());
        } catch (InvalidProtocolBufferException e) {
            return null;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Void append(List<AttachmentEvent> events) {
        var call = mutator.call("{ ? = call stereotomy.appendAttachements(?) }",
                                Collections.singletonList(JDBCType.BINARY),
                                events.stream().map(ae -> ae.getBytes()).toList());
        CompletableFuture<CallResult> submitted;
        try {
            submitted = mutator.execute(call, timeout);
        } catch (InvalidTransaction e) {
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        }
        return null;
    }

    @Override
    public List<KeyState> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        var batch = mutator.batch();
        for (KeyEvent event : events) {
            batch.execute(
            mutator.call("{ ? = call stereotomy.append(?, ?, ?) }", Collections.singletonList(JDBCType.BINARY),
                         event.getBytes(), event.getIlk(), DigestAlgorithm.DEFAULT.digestCode()));
        }
        try {
            return batch.submit(timeout)
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
                                                     .toList())
                        .get();
        } catch (InvalidTransaction | InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }

    }

    @Override
    public Void appendValidations(EventCoordinates coordinates, Map<EventCoordinates, JohnHancock> validations) {
        // TODO Auto-generated method stub
        return null;
    }

    private KeyState_ keyStateOf(byte[] b) throws InvalidProtocolBufferException {
        return KeyState_.parseFrom(b);
    }
}
