/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.security.PublicKey;
import java.security.Signature;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.ConsortiumMessage;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.proto.ID;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler.Msg;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class Consortium {

    public static class Collaborator extends Member {
        public final PublicKey consensusKey;

        public Collaborator(Member member, byte[] consensusKey) {
            this(member, publicKeyOf(consensusKey));
        }

        public Collaborator(Member member, PublicKey consensusKey) {
            super(member.getId(), member.getCertificate());
            this.consensusKey = consensusKey;
        }
    }

    public class Service {
    }

    private class Client extends State {
    }

    private abstract class CommitteeMember extends State {
    }

    private static class CurrentBlock {
        final Block   block;
        final HashKey hash;

        CurrentBlock(HashKey hash, Block block) {
            this.hash = hash;
            this.block = block;
        }
    }

    private class Follower extends CommitteeMember {
    }

    private class Leader extends CommitteeMember {
        private final Deque<Transaction> transactions = new ArrayDeque<>();
    }

    private abstract class State {

        public boolean becomeClient() {
            // TODO Auto-generated method stub
            return false;
        };

        public boolean becomeFollower() {
            // TODO Auto-generated method stub
            return false;
        };

        public boolean becomeLeader() {
            // TODO Auto-generated method stub
            return false;
        }

        public void process(Block parseFrom) {
            // TODO Auto-generated method stub

        }

        public boolean process(CurrentBlock next, Checkpoint body) {
            return false;
        }

        public boolean process(CurrentBlock next, Genesis body) {
            return false;
        }

        public boolean process(CurrentBlock next, User body) {
            return false;
        }

        public void process(ID parseFrom) {
            // TODO Auto-generated method stub

        }

        public void process(Transaction parseFrom) {
            // TODO Auto-generated method stub

        }

        public void process(Validate parseFrom) {
            // TODO Auto-generated method stub

        };

    }

    private final static Logger log = LoggerFactory.getLogger(Consortium.class);

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

    public static PublicKey publicKeyOf(byte[] consensusKey) {
        return null;
    }

    private final Communications           communications;
    private final Context<Member>          context;
    private volatile CurrentBlock          current;
    private volatile Context<Collaborator> currentView;
    private final Duration                 gossipDuration;
    private final HashKey                  id;
    private final BlockingDeque<Msg>       inbound = new LinkedBlockingDeque<>();
    private volatile Member                leader;
    private final Parameters.Builder       mConfig;
    private final Member                   member;
    private volatile Messenger             messenger;
    private final ScheduledExecutorService scheduler;
    private final Service                  service = new Service();
    private final Supplier<Signature>      signature;
    private volatile State                 state   = new Client();
    private volatile TotalOrder            totalOrder;

    public Consortium(HashKey id, Member member, Supplier<Signature> signature, Context<Member> context,
            Parameters.Builder mConfig, Communications communications, Duration gossipDuration,
            ScheduledExecutorService scheduler) {
        this.id = id;
        this.member = member;
        this.context = context;
        this.mConfig = mConfig.clone();
        this.communications = communications;
        this.signature = signature;
        this.gossipDuration = gossipDuration;
        this.scheduler = scheduler;

        context.register(membershipListener());
    }

    public boolean process(Block block) {
        final CurrentBlock previousBlock = current;
        if (block.getHeader().getHeight() != previousBlock.block.getHeader().getHeight() + 1) {
            log.error("Protocol violation.  Block height should be {} and next block height is {}",
                      previousBlock.block.getHeader().getHeight(), block.getHeader().getHeight());
            return false;
        }
        HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
        if (previousBlock.hash.equals(prev)) {
            log.error("Protocol violation. New block does not refer to current block hash. Should be {} and next block's prev is {}",
                      previousBlock.hash, prev);
            return false;
        }
        CurrentBlock next = new CurrentBlock(new HashKey(Conversion.hashOf(block.toByteArray())), block);
        current = next;
        return next();
    }

    private void becomeClient() {
        state = new Client();
    }

    private void becomeFollower() {
        state = new Follower();
    }

    private void becomeLeader() {
        state = new Leader();
    }

    private State getState() {
        final State get = state;
        return get;
    }

    private MembershipListener<Member> membershipListener() {
        return new MembershipListener<Member>() {

            @Override
            public void fail(Member member) {
                final Context<Collaborator> view = currentView;
                view.offlineIfActive(member.getId());
            }

            @Override
            public void recover(Member member) {
                final Context<Collaborator> view = currentView;
                view.activateIfOffline(member.getId());
            }
        };
    }

    private boolean next() {
        CurrentBlock next = current;
        switch (next.block.getBody().getType()) {
        case CHECKPOINT:
            return processCheckpoint(next);
        case GENESIS:
            return processGenesis(next);
        case RECONFIGURE:
            return processReconfigure(next);
        case USER:
            return processUser(next);
        case UNRECOGNIZED:
        default:
            log.info("Unrecognized block type: {} : {}", next.hashCode(), next.block);
            return false;
        }

    }

    private void pause() {
        TotalOrder previousTotalOrder = totalOrder;
        previousTotalOrder.stop();
        Messenger previousMessenger = messenger;
        previousMessenger.stop();
    }

    private void process(ConsortiumMessage message, HashKey from) {
        switch (message.getType()) {
        case BLOCK:
            try {
                getState().process(Block.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid block delivered from {}", from, e);
            }
            break;
        case PERSIST:
            try {
                getState().process(ID.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid persist delivered from {}", from, e);
            }
            break;
        case TRANSACTION:
            try {
                getState().process(Transaction.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid transaction delivered from {}", from, e);
            }
            break;
        case VALIDATE:
            try {
                getState().process(Validate.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from {}", from, e);
            }
            break;
        case UNRECOGNIZED:
        default:
            log.error("Invalid consortium message type: {} from: {}", message.getType(), from);
            break;
        }
    }

    private void process(Msg message) {
        // TODO Auto-generated method stub
    }

    private boolean processCheckpoint(CurrentBlock next) {
        Checkpoint body;
        try {
            body = Checkpoint.parseFrom(next.block.getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            log.error("Protocol violation.  Cannot decode checkpoint body: {}", e);
            return false;
        }
        return getState().process(next, body);
    }

    private boolean processGenesis(CurrentBlock next) {
        Genesis body;
        try {
            body = Genesis.parseFrom(next.block.getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            log.error("Protocol violation.  Cannot decode genesis body: {}", e);
            return false;
        }
        return getState().process(next, body);
    }

    private boolean processReconfigure(CurrentBlock next) {
        Reconfigure body;
        try {
            body = Reconfigure.parseFrom(next.block.getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
            return false;
        }
        HashKey viewId = new HashKey(body.getId());
        Context<Collaborator> newView = new Context<Collaborator>(viewId, context.getRingCount());
        body.getViewList().stream().map(v -> {
            HashKey memberId = new HashKey(v.getId());
            Member m = context.getMember(memberId);
            if (m == null) {
                return null;
            }
            return new Collaborator(m, v.getConsensusKey().toByteArray());
        }).filter(m -> m != null).forEach(m -> {
            if (context.isActive(m)) {
                newView.activate(m);
            } else {
                newView.offline(m);
            }
        });

        return viewChange(viewId, newView);
    }

    private boolean processUser(CurrentBlock next) {
        User body;
        try {
            body = User.parseFrom(next.block.getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
            return false;
        }
        return getState().process(next, body);
    }

    private void resume() {
        TotalOrder currentTO = totalOrder;
        Messenger currentMsg = messenger;
        currentTO.start();
        currentMsg.start(gossipDuration, scheduler);
    }

    private void submit(ConsortiumMessage message) {
        final Messenger currentMsgr = messenger;
        if (currentMsgr == null) {
            log.error("skipping message publish as no messenger");
            return;
        }
        messenger.publish(0, message.toByteArray());
    }

    private boolean viewChange(HashKey viewId, Context<Collaborator> newView) {
        pause();

        currentView = newView;
        messenger = new Messenger(member, signature, newView, communications, mConfig.setId(viewId).build());
        totalOrder = new TotalOrder((m, mId) -> process(m, mId), newView);
        messenger.register(0, messages -> {
            totalOrder.process(messages);
        });

        resume();

        // Live successor of the view ID on ring zero is leader
        leader = newView.ring(0).successor(viewId);

        if (member.equals(leader)) {
            return getState().becomeLeader();
        } else if (currentView.getActive().contains(member)) {
            return getState().becomeFollower();
        }
        return getState().becomeClient();
    }
}
