/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.ConsortiumMessage;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.MessageType;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

public class CollaboratorContext {
    private final Consortium                           consortium;
    private final PendingTransactions                  pending       = new PendingTransactions();
    private final TickScheduler                        scheduler     = new TickScheduler();
    private final Map<HashKey, CertifiedBlock.Builder> workingBlocks = new HashMap<>();

    CollaboratorContext(Consortium consortium) {
        this.consortium = consortium;
    }

    public void add(Transaction txn) {
        HashKey hash = new HashKey(Conversion.hashOf(txn.toByteArray()));
        pending.add(new EnqueuedTransaction(hash, txn));
    }

    public void deliverBlock(Block block, Member from) {
        Member leader = this.consortium.vState.getLeader();
        if (!from.equals(leader)) {
            this.consortium.log.debug("Rejecting block proposal from {}", from);
            return;
        }
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteArray()));
        workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder().setBlock(block));
        this.consortium.generateValidation(hash, block);
    }

    public void generateConsensusKeyPair() {
        this.consortium.vState.setConsensusKeyPair(Validator.generateKeyPair(2048, "RSA"));
    }

    public Member member() {
        return this.consortium.parameters.member;
    }

    public boolean processCheckpoint(CurrentBlock next) {
        Checkpoint body;
        try {
            body = Checkpoint.parseFrom(next.getBlock().getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            this.consortium.log.error("Protocol violation.  Cannot decode checkpoint body: {}", e);
            return false;
        }
        return body != null;
    }

    public boolean processGenesis(CurrentBlock next) {
        Genesis body;
        try {
            body = Genesis.parseFrom(next.getBlock().getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            this.consortium.log.error("Protocol violation.  Cannot decode genesis body: {}", e);
            return false;
        }
        this.consortium.transitions.genesisAccepted();
        return this.consortium.reconfigure(body.getInitialView());
    }

    public boolean processReconfigure(CurrentBlock next) {
        Reconfigure body;
        try {
            body = Reconfigure.parseFrom(next.getBlock().getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            this.consortium.log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
            return false;
        }
        return this.consortium.reconfigure(body);
    }

    public boolean processUser(CurrentBlock next) {
        User body;
        try {
            body = User.parseFrom(next.getBlock().getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            this.consortium.log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
            return false;
        }
        return body != null;
    }

    public void schedudule(Runnable action, int delta) {
        scheduler.schedule(action, delta);
    }

    public void submit(EnqueuedTransaction enqueuedTransaction) {
        if (pending.add(enqueuedTransaction)) {
            this.consortium.log.debug("Submitted txn: {}", enqueuedTransaction.getHash());
            this.consortium.deliver(ConsortiumMessage.newBuilder()
                                                     .setMsg(enqueuedTransaction.getTransaction().toByteString())
                                                     .setType(MessageType.TRANSACTION)
                                                     .build());
        }
    }

    public void tick() {
        scheduler.tick();
    }

    public void validate(Validate v) {
        HashKey hash = new HashKey(v.getHash());
        CertifiedBlock.Builder certifiedBlock = workingBlocks.get(hash);
        if (certifiedBlock == null) {
            this.consortium.log.trace("No working block to validate: {}", hash);
            return;
        }
        final Validator validator = this.consortium.vState.getValidator();
        ForkJoinPool.commonPool().execute(() -> {
            if (validator.validate(certifiedBlock.getBlock(), v)) {
                certifiedBlock.addCertifications(Certification.newBuilder()
                                                              .setId(v.getId())
                                                              .setSignature(v.getSignature()));
            }
        });
    }

    PendingTransactions getPending() {
        return pending;
    }
}
