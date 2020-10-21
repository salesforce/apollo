/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.grpc.ConsortiumClientCommunications.getCreate;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.avalanche.AvaMetrics;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.AvalancheParameters;
import com.salesforce.apollo.avalanche.Processor;
import com.salesforce.apollo.avalanche.WorkingSet.FinalizationData;
import com.salesforce.apollo.comm.CommonCommunications;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.consortium.grpc.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.grpc.ConsortiumServerCommunications;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Consortium {
    public class ConsortiumProcessor implements Processor {

        @Override
        public HashKey conflictSetOf(HashKey key, DagEntry entry) {
            Block block = manifestBlock(entry.getData().toByteArray());
            return new HashKey(block.getHeader().getPrevious());
        }

        @Override
        public void finalize(FinalizationData finalization) {
            finalizations.add(finalization);
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

        public void process(CurrentBlock next) {
            switch (next.block.getBody().getType()) {
            case CHECKPOINT: {
                Checkpoint body;
                try {
                    body = Checkpoint.parseFrom(next.block.getBody().getContents());
                } catch (InvalidProtocolBufferException e) {
                    log.error("Protocol violation.  Cannot decode checkpoint body: {}", e);
                    fail();
                    return;
                }
                process(next, body);
                break;
            }
            case GENESIS: {
                Genesis body;
                try {
                    body = Genesis.parseFrom(next.block.getBody().getContents());
                } catch (InvalidProtocolBufferException e) {
                    log.error("Protocol violation.  Cannot decode genesis body: {}", e);
                    fail();
                    return;
                }
                process(next, body);
                break;
            }
            case RECONFIGURE: {
                Reconfigure body;
                try {
                    body = Reconfigure.parseFrom(next.block.getBody().getContents());
                } catch (InvalidProtocolBufferException e) {
                    log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
                    fail();
                    return;
                }
                process(next, body);
                break;
            }
            case USER: {
                User body;
                try {
                    body = User.parseFrom(next.block.getBody().getContents());
                } catch (InvalidProtocolBufferException e) {
                    log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
                    fail();
                    return;
                }
                process(next, body);
                break;
            }
            case UNRECOGNIZED:
            default:
                break;

            }

        }

        void process(CurrentBlock next, Checkpoint body) {
        };

        void process(CurrentBlock next, Genesis body) {
        };

        void process(CurrentBlock next, Reconfigure body) {
        };

        void process(CurrentBlock next, User body) {
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

    private final Avalanche                                            avalanche;
    private final CommonCommunications<ConsortiumClientCommunications> comm;
    private volatile CurrentBlock                                      current;
    private final AtomicReference<List<Member>>                        currentView   = new AtomicReference<>();
    private final BlockingQueue<FinalizationData>                      finalizations = new LinkedBlockingQueue<>();
    @SuppressWarnings("unused")
    private final AtomicReference<Member>                              leader        = new AtomicReference<>();
    private final Service                                              service       = new Service();

    private volatile State state = new Client();

    public Consortium(Node node, Context<? extends Member> context, SecureRandom entropy, Communications communications,
            AvalancheParameters p, MetricRegistry registry) {
        avalanche = new Avalanche(node, context, entropy, communications, p,
                registry == null ? null : new AvaMetrics(registry), new ConsortiumProcessor());
        ConsortiumMetrics metrics = null;
        comm = communications.create(node, getCreate(metrics), new ConsortiumServerCommunications(service,
                communications.getClientIdentityProvider(), metrics));

    }

    @SuppressWarnings("unused")
    private void becomeClient() {
        state = new Client();
    }

    @SuppressWarnings("unused")
    private void becomeFollower() {
        state = new Follower();
    }

    @SuppressWarnings("unused")
    private void becomeLeader() {
        state = new Leader();
    }

    private void fail() {
        // TODO Auto-generated method stub

    }

    @SuppressWarnings("unused")
    private State getState() {
        final State get = state;
        return get;
    }

    private ConsortiumClientCommunications linkFor(Member m) {
        try {
            return comm.apply(m, avalanche.getNode());
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    @SuppressWarnings("unused")
    private void multicast(Consumer<ConsortiumClientCommunications> broadcast) {
        currentView.get().forEach(e -> {
            ConsortiumClientCommunications ch = linkFor(e);
            try {
                broadcast.accept(ch);
            } finally {
                ch.release();
            }
        });
    }

    @SuppressWarnings("unused")
    private List<Block> next() {
        FinalizationData finalized;
        try {
            finalized = finalizations.poll(1, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            log.debug("interrupted polling for next block");
            return null;
        }
        if (finalized == null) {
            return null;
        }
        List<Block> blocks = finalized.finalized.stream()
                                                .map(e -> Conversion.manifestDag(e.entry))
                                                .filter(e -> e != null)
                                                .map(e -> manifestBlock(e.getData().toByteArray()))
                                                .filter(e -> e != null)
                                                .collect(Collectors.toList());
        Collections.sort(blocks, (a, b) -> Long.compare(a.getHeader().getHeight(), b.getHeader().getHeight()));
        return blocks;
    }

    @SuppressWarnings("unused")
    private void process(Block block) {
        final CurrentBlock previousBlock = current;
        if (block.getHeader().getHeight() != previousBlock.block.getHeader().getHeight() + 1) {
            log.error("Protocol violation.  Block height should be {} and next block height is {}",
                      previousBlock.block.getHeader().getHeight(), block.getHeader().getHeight());
            fail();
            return;
        }
        HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
        if (previousBlock.hash.equals(prev)) {
            log.error("Protocol violation. New block does not refer to current block hash. Should be {} and next block's prev is {}",
                      previousBlock.hash, prev);
            fail();
            return;
        }
        CurrentBlock next = new CurrentBlock(new HashKey(Conversion.hashOf(block.toByteArray())), block);
        current = next;
        state.process(next);
    }
}
