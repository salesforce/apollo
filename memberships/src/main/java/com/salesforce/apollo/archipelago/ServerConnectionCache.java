/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Privides a safe mechanism for caching expensive connections to a server. We use MTLS, so we want to make good use of
 * the ManagedChannels. Fireflies, by its nature, will keep some subset of connections open for gossip use, based on a
 * ring. Avalanche samples a random subset of known servers. Ghost has access patterns based on hahes. And so on.
 * <p>
 * This cache allows grpc clients to reuse the underlying ManagedChannel as "Bob" inteneded, enforcing some upper limit
 * on the connections used.
 * <p>
 * ManagedChannels are never closed while they are open and used by a client stub. Connections can be opened up to some
 * total limit, which does not have to be the target number of open + idle connections. ManagedChannels in the cache
 * keep track of their overall usage count by client stubs - each borrow increments this usage count.
 * <p>
 * When ManagedChannels are closed, they are closed in the order of least usage count. ManagedChannels may also have a
 * minimum idle duration, to prevent cache thrashing. When this duration is > 0, the connection will not be closed,
 * potentially overshooting target cache counts
 *
 * @author hal.hildebrand
 */
public class ServerConnectionCache {

    private final static Logger                                  log   = LoggerFactory.getLogger(
    ServerConnectionCache.class);
    private final        Map<Member, ReleasableManagedChannel>   cache = new HashMap<>();
    private final        Clock                                   clock;
    private final        ServerConnectionFactory                 factory;
    private final        ReentrantLock                           lock  = new ReentrantLock(true);
    private final        ServerConnectionCacheMetrics            metrics;
    private final        Duration                                minIdle;
    private final        PriorityQueue<ReleasableManagedChannel> queue = new PriorityQueue<>();
    private final        int                                     target;
    private final        Digest                                  member;

    public ServerConnectionCache(Digest member, ServerConnectionFactory factory, int target, Duration minIdle,
                                 Clock clock, ServerConnectionCacheMetrics metrics) {
        assert member != null;
        this.factory = factory;
        this.target = Math.max(target, 1);
        this.minIdle = minIdle;
        this.clock = clock;
        this.metrics = metrics;
        this.member = member;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public ManagedServerChannel borrow(Digest context, Member to) {
        return lock(() -> {
            if (cache.size() >= target) {
                log.debug("Cache target open connections exceeded: {}, opening to: {} on: {}", target, to.getId(),
                          member);
            }
            ReleasableManagedChannel connection = cache.computeIfAbsent(to, m -> {
                log.debug("Creating new channel to: {} on: {}", to.getId(), m.getId());
                ManagedChannel channel;
                try {
                    channel = factory.connectTo(to);
                } catch (Throwable t) {
                    log.error("Cannot connect to: {} on: {}", to.getId(), member, t);
                    return null;
                }
                ReleasableManagedChannel conn = new ReleasableManagedChannel(to, channel, member);
                if (metrics != null) {
                    metrics.createConnection().inc();
                    metrics.openConnections().inc();
                }
                return conn;
            });
            if (connection.incrementBorrow()) {
                log.debug("Increment borrow to: {} channel to: {} on: {}", connection.borrowed,
                          connection.member.getId(), member);
                if (metrics != null) {
                    metrics.borrowRate().mark();
                }
                queue.remove(connection);
            }
            log.trace("Borrowed channel to: {}, borrowed: {}, usage: {} on: {}", connection.member.getId(),
                      connection.borrowed, connection.usageCount, member);
            return new ManagedServerChannel(context, connection);
        });
    }

    public <T> T borrow(Digest context, Member to, CreateClientCommunications<T> createFunction) {
        return createFunction.create(borrow(context, to));
    }

    public void close() {
        lock(() -> {
            log.info("Closing connection cache on: {}", member);
            for (ReleasableManagedChannel conn : new ArrayList<>(cache.values())) {
                try {
                    conn.channel.shutdownNow();
                    if (metrics != null) {
                        metrics.channelOpenDuration().update(Duration.between(conn.created, Instant.now(clock)));
                        metrics.openConnections().dec();
                    }
                } catch (Throwable e) {
                    log.debug("Error closing connection to: {} on: {}", conn.member.getId(), member);
                }
            }
            cache.clear();
            queue.clear();
            return null;
        });
    }

    public void release(ReleasableManagedChannel connection) {
        lock(() -> {
            if (connection.decrementBorrow()) {
                log.debug("Releasing connection to: {} on: {}", connection.member.getId(), member);
                queue.add(connection);
                if (metrics != null) {
                    metrics.releaseRate().mark();
                }
                manageConnections();
            }
            return null;
        });
    }

    private boolean close(ReleasableManagedChannel connection) {
        if (connection.isCloseable()) {
            try {
                connection.channel.shutdownNow();
            } catch (Throwable t) {
                log.debug("Error closing connection to: {} on: {}", connection.member.getId(), connection.member);
            }
            log.debug("connection to: {} is closed on: {}", connection.member.getId(), member);
            cache.remove(connection.member);
            if (metrics != null) {
                metrics.openConnections().dec();
                metrics.closeConnectionRate().mark();
                metrics.channelOpenDuration().update(Duration.between(connection.created, Instant.now(clock)));
            }
            return true;
        }
        return false;
    }

    private <T> T lock(Supplier<T> supplier) {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    private void manageConnections() {
        log.debug("Managing connections: {} idle: {} on: {}", cache.size(), queue.size(), member);
        Iterator<ReleasableManagedChannel> connections = queue.iterator();
        while (connections.hasNext() && cache.size() > target) {
            if (close(connections.next())) {
                connections.remove();
            }
        }
    }

    @FunctionalInterface
    public interface CreateClientCommunications<Client> {
        Client create(ManagedServerChannel channel);
    }

    public interface ServerConnectionCacheMetrics {

        Meter borrowRate();

        Timer channelOpenDuration();

        Meter closeConnectionRate();

        Counter createConnection();

        Meter failedConnectionRate();

        Counter failedOpenConnection();

        Counter openConnections();

        Meter releaseRate();

    }

    public interface ServerConnectionFactory {
        ManagedChannel connectTo(Member to);
    }

    public static class Builder implements Cloneable {
        private Clock                        clock   = Clock.systemUTC();
        private ServerConnectionFactory      factory = null;
        private ServerConnectionCacheMetrics metrics;
        private Duration                     minIdle = Duration.ofMillis(100);
        private int                          target  = 10;
        private Digest                       member;

        public ServerConnectionCache build() {
            return new ServerConnectionCache(member, factory, target, minIdle, clock, metrics);
        }

        @Override
        public Builder clone() {
            try {
                return (Builder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException(e);
            }
        }

        public Clock getClock() {
            return clock;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public ServerConnectionFactory getFactory() {
            return factory;
        }

        public Builder setFactory(ServerConnectionFactory factory) {
            this.factory = factory;
            return this;
        }

        public Digest getMember() {
            return member;
        }

        public Builder setMember(Digest member) {
            this.member = member;
            return this;
        }

        public ServerConnectionCacheMetrics getMetrics() {
            return metrics;
        }

        public Builder setMetrics(ServerConnectionCacheMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Duration getMinIdle() {
            return minIdle;
        }

        public Builder setMinIdle(Duration minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public int getTarget() {
            return target;
        }

        public Builder setTarget(int target) {
            this.target = target;
            return this;
        }
    }

    class ReleasableManagedChannel implements Comparable<ReleasableManagedChannel> {
        private final    AtomicInteger  borrowed   = new AtomicInteger();
        private final    ManagedChannel channel;
        private final    Instant        created;
        private final    Member         member;
        private final    AtomicInteger  usageCount = new AtomicInteger();
        private final    Digest         from;
        private volatile Instant        lastUsed;

        public ReleasableManagedChannel(Member member, ManagedChannel channel, Digest from) {
            this.member = member;
            this.channel = channel;
            created = Instant.now(clock);
            lastUsed = Instant.now(clock);
            this.from = from;
        }

        @Override
        public int compareTo(ReleasableManagedChannel o) {
            return Integer.compare(usageCount.get(), o.usageCount.get());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if ((obj == null) || (getClass() != obj.getClass()))
                return false;
            return member.equals(((ReleasableManagedChannel) obj).member);
        }

        public ManagedChannel getChannel() {
            return channel;
        }

        public Digest getFrom() {
            return from;
        }

        public Member getMember() {
            return member;
        }

        @Override
        public int hashCode() {
            return member.hashCode();
        }

        public boolean isCloseable() {
            return lastUsed.plus(minIdle).isBefore(Instant.now(clock));
        }

        public void release() {
            log.trace("Release connection to: {} on: {}", getMember().getId(), getFrom());
            ServerConnectionCache.this.release(this);
        }

        public ManagedChannel shutdown() {
            throw new IllegalStateException("Should not be called");
        }

        public ManagedChannel shutdownNow() {
            throw new IllegalStateException("Should not be called");
        }

        private boolean decrementBorrow() {
            if (borrowed.decrementAndGet() == 0) {
                lastUsed = Instant.now(clock);
                return true;
            }
            return false;
        }

        private boolean incrementBorrow() {
            usageCount.incrementAndGet();
            return borrowed.incrementAndGet() == 1;
        }
    }
}
