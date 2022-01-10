/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.joou.ULong;

import com.google.common.util.concurrent.SettableFuture;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Session;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

import io.grpc.Status;

/**
 * Single node emulation of the SQL State Machine for testing, development, etc.
 * 
 * @author hal.hildebrand
 *
 */
public class Emulator {

    private final AtomicReference<Digest> hash;
    private final AtomicLong              height   = new AtomicLong(0);
    private final ReentrantLock           lock     = new ReentrantLock();
    private final Mutator                 mutator;
    private final Parameters              params;
    private final SqlStateMachine         ssm;
    private final AtomicBoolean           started  = new AtomicBoolean();
    private final TransactionExecutor     txnExec;
    private final AtomicInteger           txnIndex = new AtomicInteger(0);

    public Emulator() throws IOException {
        this(DigestAlgorithm.DEFAULT.getOrigin().prefix(Utils.bitStreamEntropy().nextLong()));
    }

    public Emulator(Digest base) throws IOException {
        this(new SqlStateMachine(String.format("jdbc:h2:mem:emulation-%s-%s", base,
                                               Utils.bitStreamEntropy().nextLong()),
                                 new Properties(), Files.createTempDirectory("emulation").toFile()),
             base);
    }

    public Emulator(SqlStateMachine ssm, Digest base) {
        this.ssm = ssm;
        txnExec = this.ssm.getExecutor();
        hash = new AtomicReference<>(base);
        params = Parameters.newBuilder()
                           .setMember(new SigningMemberImpl(Utils.getMember(0)))
                           .setContext(new Context<>(base, 0.01, 5, 3))
                           .build();
        var algorithm = base.getAlgorithm();
        Session session = new Session(params, st -> {
            lock.lock();
            try {
                SettableFuture<Status> f = SettableFuture.create();
                Transaction txn = st.transaction();
                txnExec.execute(txnIndex.incrementAndGet(), CHOAM.hashOf(txn, algorithm), txn, st.onCompletion());
                f.set(Status.OK);
                return f;
            } finally {
                lock.unlock();
            }
        });
        mutator = ssm.getMutator(session);
    }

    public Mutator getMutator() {
        if (!started.get()) {
            throw new IllegalStateException("Emulation has not been started");
        }
        return mutator;
    }

    public void newBlock() {
        lock.lock();
        try {
            var h = height.incrementAndGet();
            txnIndex.set(0);
            txnExec.beginBlock(ULong.valueOf(h), hash.updateAndGet(d -> d.prefix(h)));
        } finally {
            lock.unlock();
        }
    }

    public Connection newConnector() {
        if (!started.get()) {
            throw new IllegalStateException("Emulation has not been started");
        }
        return ssm.newConnection();
    }

    public void start(Txn... genesisTransactions) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        txnExec.genesis(hash.updateAndGet(d -> d.prefix(0)), CHOAM.toGenesisData(Arrays.asList(genesisTransactions)));
    }
}
