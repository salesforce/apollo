/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.ConsortiumMessage;
import com.salesfoce.apollo.consortium.proto.MessageType;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.proto.ID;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.comms.ConsortiumServerCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler.Msg;
import com.salesforce.apollo.membership.messaging.TotalOrder;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Consortium {

    public class Service {

        public TransactionResult clientSubmit(SubmitTransaction request) {
            HashKey hash = new HashKey(Conversion.hashOf(request.getTransaction().toByteArray()));
            transitions.submit(new EnqueuedTransaction(hash, request.getTransaction()));
            return TransactionResult.getDefaultInstance();
        }

    }

    private final static Logger DEFAULT_LOGGER = LoggerFactory.getLogger(Consortium.class);

    public static Block manifestBlock(byte[] data) {
        if (data.length == 0) {
            System.out.println(" Invalid data");
        }
        try {
            return Block.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid data");
        }
    }

    Logger                                                                                         log       = DEFAULT_LOGGER;
    final Parameters                                                                               parameters;
    final Transitions                                                                              transitions;
    final VolatileState                                                                            vState    = new VolatileState();
    private final Function<HashKey, CommonCommunications<ConsortiumClientCommunications, Service>> createClientComms;
    private final Fsm<CollaboratorContext, Transitions>                                            fsm;
    private final Map<HashKey, SubmittedTransaction>                                               submitted = new ConcurrentHashMap<>();

    public Consortium(Parameters parameters) {
        this.parameters = parameters;
        this.createClientComms = k -> parameters.communications.create(parameters.member, k, new Service(),
                                                                       r -> new ConsortiumServerCommunications(
                                                                               parameters.communications.getClientIdentityProvider(),
                                                                               null, r),
                                                                       ConsortiumClientCommunications.getCreate(null));
        parameters.context.register(vState);
        fsm = Fsm.construct(new CollaboratorContext(this), Transitions.class, CollaboratorFsm.INITIAL, true);
        transitions = fsm.getTransitions();
    }

    public Logger getLog() {
        return log;
    }

    public Member getMember() {
        return parameters.member;
    }

    public CollaboratorContext getState() {
        return fsm.getContext();
    }

    public void process(CertifiedBlock certifiedBlock) {
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteArray()));
        log.info("Processing block {} : {}", hash, block.getBody().getType());
        final CurrentBlock previousBlock = vState.getCurrent();
        final Validator v = vState.getValidator();
        if (previousBlock != null) {
            if (block.getHeader().getHeight() != previousBlock.getBlock().getHeader().getHeight() + 1) {
                log.error("Protocol violation.  Block height should be {} and next block height is {}",
                          previousBlock.getBlock().getHeader().getHeight(), block.getHeader().getHeight());
                return;
            }
            HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
            if (previousBlock.getHash().equals(prev)) {
                log.error("Protocol violation. New block does not refer to current block hash. Should be {} and next block's prev is {}",
                          previousBlock.getHash(), prev);
                return;
            }
            if (!v.validate(certifiedBlock)) {
                log.error("Protocol violation. New block is not validated {}", certifiedBlock);
                return;
            }
        } else {
            if (block.getBody().getType() != BodyType.GENESIS) {
                log.error("Invalid genesis block: {}", block.getBody().getType());
                return;
            }
            if (!Validator.validateGenesis(certifiedBlock)) {
                log.error("Protocol violation. Genesis block is not validated {}", hash);
                return;
            }
        }
        vState.setCurrent(new CurrentBlock(hash, block));
        next();
    }

    public void setLog(Logger log) {
        this.log = log;
    }

    public void start() {
        vState.resume(new Service(), parameters.gossipDuration, parameters.scheduler);
    }

    public void stop() {
        vState.pause();
    }

    public HashKey submit(List<byte[]> transactions, Consumer<HashKey> onCompletion) throws TimeoutException {
        final Context<Collaborator> current = vState.getCurrentView();
        if (current == null) {
            throw new IllegalStateException("The current view is undefined, unable to process transactions");
        }

        byte[] nonce = new byte[32];
        parameters.msgParameters.entropy.nextBytes(nonce);
        ByteBuffer signed = ByteBuffer.allocate(nonce.length + HashKey.BYTE_SIZE
                + transactions.stream().mapToInt(e -> e.length).sum());

        signed.put(parameters.member.getId().bytes());
        signed.put(nonce);

        Transaction.Builder builder = Transaction.newBuilder()
                                                 .setSource(parameters.member.getId().toByteString())
                                                 .setNonce(ByteString.copyFrom(nonce));
        transactions.forEach(t -> {
            builder.addBatch(ByteString.copyFrom(t));
            signed.put(t);
        });

        byte[] hash = Conversion.hashOf(signed.array());

        byte[] signature = Validator.sign(parameters.signature.get(), parameters.msgParameters.entropy, hash);
        if (signature == null) {
            throw new IllegalStateException("Unable to sign transaction batch");
        }
        builder.setSignature(ByteString.copyFrom(signature));
        HashKey hashKey = new HashKey(hash);
        Transaction transaction = builder.build();
        submitted.put(hashKey, new SubmittedTransaction(transaction, onCompletion));
        int toleranceLevel = vState.getToleranceLevel();
        List<TransactionResult> results = current.sample(toleranceLevel + 1, parameters.msgParameters.entropy,
                                                         parameters.member.getId())
                                                 .stream()
                                                 .map(c -> {
                                                     ConsortiumClientCommunications link = linkFor(c);
                                                     if (link == null) {
                                                         log.warn("Cannot get link for {}", c.getId());
                                                         return null;
                                                     }
                                                     return link.clientSubmit(SubmitTransaction.newBuilder()
                                                                                               .setContext(current.getId()
                                                                                                                  .toByteString())
                                                                                               .setTransaction(transaction)
                                                                                               .build());
                                                 })
                                                 .filter(r -> r != null)
                                                 .limit(toleranceLevel)
                                                 .collect(Collectors.toList());
        if (results.size() < toleranceLevel) {
            throw new TimeoutException("Cannot submit transaction " + hashKey);
        }
        return hashKey;
    }

    void deliver(ConsortiumMessage message) {
        final Messenger currentMsgr = vState.getMessenger();
        if (currentMsgr == null) {
            log.error("skipping message publish as no messenger");
            return;
        }
        currentMsgr.publish(message.toByteArray());
    }

    void generateValidation(HashKey hash, Block block) {
        byte[] signature = Validator.sign(vState.getConsensusKeyPair().getPrivate(), parameters.msgParameters.entropy,
                                          Conversion.hashOf(block.getHeader().toByteArray()));
        if (signature == null) {
            log.error("Unable to sign block {}", hash);
            return;
        }
        Validate validation;
        validation = Validate.newBuilder()
                             .setId(parameters.member.getId().toByteString())
                             .setHash(hash.toByteString())
                             .setSignature(ByteString.copyFrom(signature))
                             .build();
        vState.getMessenger().publish(ConsortiumMessage.newBuilder()
                                                  .setType(MessageType.VALIDATE)
                                                  .setMsg(validation.toByteString())
                                                  .build()
                                                  .toByteArray());
    }

    boolean reconfigure(Reconfigure body) {
        HashKey viewId = new HashKey(body.getId());
        Context<Collaborator> newView = new Context<Collaborator>(viewId, parameters.context.toleranceLevel() + 1);
        body.getViewList().stream().map(vm -> {
            HashKey memberId = new HashKey(vm.getId());
            Member m = parameters.context.getMember(memberId);
            if (m == null) {
                return null;
            }
            return new Collaborator(m, vm.getConsensusKey().toByteArray());
        }).filter(m -> m != null).forEach(m -> {
            if (parameters.context.isActive(m)) {
                newView.activate(m);
            } else {
                newView.offline(m);
            }
        });

        return viewChange(newView, body.getToleranceLevel());
    }

    private ConsortiumClientCommunications linkFor(Member m) {
        try {
            return vState.getComm().apply(m, parameters.member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private void next() {
        CurrentBlock next = vState.getCurrent();
        switch (next.getBlock().getBody().getType()) {
        case CHECKPOINT:
            transitions.processCheckpoint(next);
            break;
        case GENESIS:
            transitions.processGenesis(next);
            break;
        case RECONFIGURE:
            transitions.processReconfigure(next);
            break;
        case USER:
            transitions.processUser(next);
            break;
        case UNRECOGNIZED:
        default:
            log.error("Unrecognized block type: {} : {}", next.hashCode(), next.getBlock());
        }

    }

    private void process(Msg msg) {
        log.trace("Processing {} from {}", msg.sequenceNumber, msg.from);
        ConsortiumMessage message;
        try {
            message = ConsortiumMessage.parseFrom(msg.content);
        } catch (InvalidProtocolBufferException e) {
            log.error("error parsing message from {}", msg.from, e);
            return;
        }
        switch (message.getType()) {
        case BLOCK:
            try {
                transitions.deliverBlock(Block.parseFrom(message.getMsg().asReadOnlyByteBuffer()), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid block delivered from {}", msg.from, e);
            }
            break;
        case PERSIST:
            try {
                transitions.deliverPersist(ID.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid persist delivered from {}", msg.from, e);
            }
            break;
        case TRANSACTION:
            try {
                transitions.deliverTransaction(Transaction.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid transaction delivered from {}", msg.from, e);
            }
            break;
        case VALIDATE:
            try {
                transitions.deliverValidate(Validate.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from {}", msg.from, e);
            }
            break;
        case UNRECOGNIZED:
        default:
            log.error("Invalid consortium message type: {} from: {}", message.getType(), msg.from);
            break;
        }
    }

    /**
     * Ye Jesus Nut
     */
    private boolean viewChange(Context<Collaborator> newView, int t) {
        vState.pause();

        log.trace("View rings: {} ttl: {}", newView.getRingCount(), newView.timeToLive());

        // Live successor of the view ID on ring zero is presumed leader
        Collaborator newLeader = newView.ring(0).successor(newView.getId());

        vState.setComm(createClientComms.apply(newView.getId()));
        vState.setValidator(new Validator(newLeader, newView, t));
        vState.setMessenger(null);
        vState.setTO(null);

        if (newView.getMember(parameters.member.getId()) != null) { // cohort member
            Messenger nextMsgr = new Messenger(parameters.member, parameters.signature, newView,
                    parameters.communications, parameters.msgParameters);
            vState.setMessenger(nextMsgr);
            vState.setTO(new TotalOrder((m, k) -> process(m), newView));
            nextMsgr.register(() -> fsm.getContext().tick());
            nextMsgr.register(messages -> vState.getTO().process(messages));
            if (parameters.member.equals(newLeader)) { // I yam what I yam
                log.info("reconfiguring, becoming leader: {}", parameters.member);
                transitions.becomeLeader();
            } else {
                log.info("reconfiguring, becoming follower: {}", parameters.member);
                transitions.becomeFollower();
            }
        } else { // you are all my puppets
            log.info("reconfiguring, becoming client: {}", parameters.member);
            transitions.becomeClient();
        }

        vState.resume(new Service(), parameters.gossipDuration, parameters.scheduler);
        return true;
    }
}
