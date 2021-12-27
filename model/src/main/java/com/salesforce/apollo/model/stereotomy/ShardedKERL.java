/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.stereotomy;

import java.sql.Connection;
import java.sql.Types;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    private final Mutator            mutator;
    private Executor                 exec;
    private Duration                 timeout;
    private ScheduledExecutorService scheduler;

    public ShardedKERL(Connection connection, Mutator mutator, DigestAlgorithm digestAlgorithm) {
        super(connection, digestAlgorithm);
        this.mutator = mutator;
    }

    @Override
    public void append(AttachmentEvent event, KeyState newState) {
        // TODO Auto-generated method stub

    }

    @Override
    public KeyState append(KeyEvent event) {
        var call = mutator.call("{ ? = call stereotomy_kerl.append(?, ?, ?) }", Collections.singletonList(Types.BINARY),
                                new Object[] { event.getBytes(), event.getIlk(),
                                               DigestAlgorithm.DEFAULT.digestCode() });
        CallResult callResult = null;
        CompletableFuture<CallResult> submitted;
        try {
            submitted = mutator.execute(exec, call, timeout, scheduler);
        } catch (InvalidTransaction e) {
            throw new IllegalStateException("Cannot append keystate, error submitting transaction", e);
        }
        try {
            callResult = submitted.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        byte[] keyState = (byte[]) callResult.outValues.get(0);
        if (keyState == null) {
            throw new IllegalStateException("Cannot append keystate");
        }
        try {
            return new KeyStateImpl(keyState);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Cannot deserialize resulting keystate", e);
        }
    }
}
