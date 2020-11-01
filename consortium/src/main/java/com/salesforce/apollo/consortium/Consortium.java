/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.security.PublicKey;
import java.security.Signature;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class Consortium {

    public static class Collaborator {
        public final PublicKey consensusKey;
        public final Member    member;

        public Collaborator(Member member, byte[] consensusKey) {
            this(member, publicKeyOf(consensusKey));
        }

        public Collaborator(Member member, PublicKey consensusKey) {
            this.member = member;
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

        boolean process(CurrentBlock next, Checkpoint body) {
            return false;
        }

        boolean process(CurrentBlock next, Genesis body) {
            return false;
        }

        boolean process(CurrentBlock next, User body) {
            return false;
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

    private final Communications         communications;
    private final Map<Member, PublicKey> consensusKeys = new HashMap<>();
    private final Context<Member>        context;
    private volatile CurrentBlock        current;
    private volatile Context<Member>     currentView;
    private final HashKey                id;
    private volatile Member              leader;
    private final Parameters.Builder     mConfig;
    private final Member                 member;
    private volatile Messenger           messenger;
    private final Service                service       = new Service();
    private final Supplier<Signature>    signature;
    private volatile State               state         = new Client();

    public Consortium(HashKey id, Member member, Supplier<Signature> signature, Context<Member> context,
            Parameters.Builder mConfig, Communications communications) {
        this.id = id;
        this.member = member;
        this.context = context;
        this.mConfig = mConfig.clone();
        this.communications = communications;
        this.signature = signature;
        context.register(membershipListener());
    }

    public void process(Block block) {
        final CurrentBlock previousBlock = current;
        if (block.getHeader().getHeight() != previousBlock.block.getHeader().getHeight() + 1) {
            log.error("Protocol violation.  Block height should be {} and next block height is {}",
                      previousBlock.block.getHeader().getHeight(), block.getHeader().getHeight());
            return;
        }
        HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
        if (previousBlock.hash.equals(prev)) {
            log.error("Protocol violation. New block does not refer to current block hash. Should be {} and next block's prev is {}",
                      previousBlock.hash, prev);
            return;
        }
        CurrentBlock next = new CurrentBlock(new HashKey(Conversion.hashOf(block.toByteArray())), block);
        current = next;
        next();
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
                final Context<Member> view = currentView;
                view.offlineIfActive(member);
            }

            @Override
            public void recover(Member member) {
                final Context<Member> view = currentView;
                view.activateIfOffline(member);
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
        Context<Member> newView = new Context<Member>(viewId, context.getRingCount());
        Map<Member, PublicKey> keys = new HashMap<>();
        body.getViewList().stream().map(v -> {
            HashKey memberId = new HashKey(v.getId());
            Member m = context.getMember(memberId);
            if (m == null) {
                return null;
            }
            keys.put(m, publicKeyOf(v.getConsensusKey().toByteArray()));
            return m;
        }).filter(m -> m != null).forEach(m -> {
            if (context.isActive(m)) {
                newView.activate(m);
            } else {
                newView.offline(m);
            }
        });

        return viewChange(viewId, newView, keys);
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

    private boolean viewChange(HashKey viewId, Context<Member> newView, Map<Member, PublicKey> keys) {
        currentView = newView;
        consensusKeys.clear();
        consensusKeys.putAll(keys);
        messenger = new Messenger(member, signature, newView, communications, mConfig.setId(viewId).build());

        // Live successor of the view ID on ring zero is leader
        leader = newView.successors(viewId).get(0);

        if (member.equals(leader)) {
            return getState().becomeLeader();
        } else if (currentView.getActive().contains(member)) {
            return getState().becomeFollower();
        }
        return getState().becomeClient();
    }
}
