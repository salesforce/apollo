/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.Session;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.choam.proto.SubmitResult.Result;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.context.DynamicContextImpl;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.state.proto.Txn;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import org.joou.ULong;

import java.io.IOException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single node emulation of the SQL State Machine for testing, development, etc.
 *
 * @author hal.hildebrand
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
        this(DigestAlgorithm.DEFAULT.getOrigin().prefix(Entropy.nextBitsStreamLong()));
    }

    public Emulator(Digest base) throws IOException {
        this(new SqlStateMachine(String.format("jdbc:h2:mem:emulation-%s-%s", base, Entropy.nextBitsStreamLong()),
                                 new Properties(), Files.createTempDirectory("emulation").toFile()), base);
    }

    public Emulator(SqlStateMachine ssm, Digest base) {
        this.ssm = ssm;
        txnExec = this.ssm.getExecutor();
        hash = new AtomicReference<>(base);
        SecureRandom entropy;
        try {
            entropy = SecureRandom.getInstance("SHA1PRNG");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        entropy.setSeed(new byte[] { 6, 6, 6 });
        ControlledIdentifier<SelfAddressingIdentifier> identifier;
        identifier = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT),
                                        entropy).newIdentifier();
        params = Parameters.newBuilder()
                           .setGenerateGenesis(true)
                           .build(RuntimeParameters.newBuilder()
                                                   .setMember(new ControlledIdentifierMember(identifier))
                                                   .setContext(new DynamicContextImpl<>(base, 5, 0.01, 3))
                                                   .build());
        var algorithm = base.getAlgorithm();
        Session session = new Session(params, st -> {
            lock.lock();
            try {
                Transaction txn = st.transaction();
                txnExec.execute(txnIndex.incrementAndGet(), CHOAM.hashOf(txn, algorithm), txn, st.onCompletion(),
                                r -> r.run());
                return SubmitResult.newBuilder().setResult(Result.PUBLISHED).build();
            } finally {
                lock.unlock();
            }
        });
        session.setView(new HashedCertifiedBlock(DigestAlgorithm.DEFAULT, CertifiedBlock.newBuilder()
                                                                                        .setBlock(Block.newBuilder()
                                                                                                       .setHeader(
                                                                                                       Header.newBuilder()
                                                                                                             .setHeight(
                                                                                                             100)))
                                                                                        .build()));
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
