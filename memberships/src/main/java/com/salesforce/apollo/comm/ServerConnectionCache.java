/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.ManagedChannel;

/**
 * 
 * Privides a safe mechanism for caching expensive connections to a server. We
 * use MTLS, so we want to make good use of the ManagedChannels. Fireflies, by
 * its nature, will keep some subset of connections open for gossip use, based
 * on a ring. Avalanche samples a random subset of known servers. Ghost has
 * access patterns based on hahes. And so on.
 * <p>
 * This cache allows grpc clients to reuse the underlying ManagedChannel as
 * "Bob" inteneded, enforcing some upper limit on the connections used.
 * <p>
 * ManagedChannels are never closed while they are open and used by a client
 * stub. Connections can be opened up to some total limit, which does not have
 * to be the target number of open + idle connections. ManagedChannels in the
 * cache keep track of their overall usage count by client stubs - each borrow
 * increments this usage count.
 * <p>
 * When ManagedChannels are closed, they are closed in the order of least usage
 * count. ManagedChannels may also have a minimum idle duration, to prevent
 * cache thrashing. When this duration is > 0, the connection will not be
 * closed, potentially overshooting target cache counts
 * 
 * @author hal.hildebrand
 *
 */
public class ServerConnectionCache {

    @FunctionalInterface
    public static interface CreateClientCommunications<T> {
        T create(Member to, Member from, ManagedServerConnection connection);
    }

    public class ManagedServerConnection implements Comparable<ManagedServerConnection> {
        public final ManagedChannel channel;
        public final HashKey        id;
        private volatile int        borrowed   = 0;
        private volatile Instant    lastUsed   = Instant.now(clock);
        private volatile int        usageCount = 0;

        public ManagedServerConnection(HashKey id, ManagedChannel channel) {
            this.id = id;
            this.channel = channel;
        }

        @Override
        public int compareTo(ManagedServerConnection o) {
            return Integer.compare(usageCount, o.usageCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ManagedServerConnection other = (ManagedServerConnection) obj;
            return id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        public boolean isCloseable() {
            return lastUsed.plus(minIdle).isBefore(Instant.now(clock));
        }

        public void release() {
            ServerConnectionCache.this.release(this);
        }

        private boolean decrementBorrow() {
            borrowed = borrowed - 1;
            if (borrowed == 0) {
                lastUsed = Instant.now(clock);
                return true;
            }
            return false;
        }

        private boolean incrementBorrow() {
            borrowed = borrowed + 1;
            usageCount = usageCount + 1;
            return borrowed == 1;
        }
    }

    public static class ServerConnectionCacheBuilder {
        private Clock                   clock             = Clock.systemUTC();
        private ServerConnectionFactory factory           = null;
        private Duration                minIdle           = Duration.ofMillis(100);
        private int                     target            = 0;
        private int                     targetOpenAndIdle = 0;

        public ServerConnectionCache build() {
            return new ServerConnectionCache(factory, target, minIdle, clock, targetOpenAndIdle);
        }

        public Clock getClock() {
            return clock;
        }

        public ServerConnectionFactory getFactory() {
            return factory;
        }

        public Duration getMinIdle() {
            return minIdle;
        }

        public int getTarget() {
            return target;
        }

        public int getTargetOpenAndIdle() {
            return targetOpenAndIdle;
        }

        public ServerConnectionCacheBuilder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public ServerConnectionCacheBuilder setFactory(ServerConnectionFactory factory) {
            this.factory = factory;
            return this;
        }

        public ServerConnectionCacheBuilder setMinIdle(Duration minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public ServerConnectionCacheBuilder setTarget(int target) {
            this.target = target;
            return this;
        }

        public ServerConnectionCacheBuilder setTargetOpenAndIdle(int targetOpenAndIdle) {
            this.targetOpenAndIdle = targetOpenAndIdle;
            return this;
        }
    }

    public static interface ServerConnectionFactory {
        ManagedChannel connectTo(Member to, Member from);
    }

    private final static Logger log = LoggerFactory.getLogger(ServerConnectionCache.class);

    public static ServerConnectionCacheBuilder newBuilder() {
        return new ServerConnectionCacheBuilder();
    }

    private final Map<HashKey, ManagedServerConnection>  cache = new HashMap<>();
    private final Clock                                  clock;
    private final ServerConnectionFactory                factory;
    private final ReadWriteLock                          lock  = new ReentrantReadWriteLock(true);
    private final Duration                               minIdle;
    private final PriorityQueue<ManagedServerConnection> queue = new PriorityQueue<>();
    private final int                                    target;
    private final int                                    targetOpenAndIdle;

    public ServerConnectionCache(ServerConnectionFactory factory, int target, Duration minIdle, Clock clock,
            int targetOpenAndIdle) {
        this.factory = factory;
        this.target = target;
        this.minIdle = minIdle;
        this.targetOpenAndIdle = targetOpenAndIdle;
        this.clock = clock;
    }

    public <T> T borrow(Member to, Member from, CreateClientCommunications<T> createFunction) {
        return lock(() -> {
            if (cache.size() == target) {
                log.warn("Cache target open connections exceeded: {} from: {}, failed to open channel to {}", target,
                         from, to.getId());
                return null;
            }
            ManagedServerConnection connection = cache.computeIfAbsent(to.getId(),
                                                                       member -> new ManagedServerConnection(to.getId(),
                                                                               factory.connectTo(to, from)));
            if (connection == null) {
                log.warn("Failed to open channel to {} from {}", to.getId(), from.getId());
                return null;
            }
            if (connection.incrementBorrow()) {
                log.debug("Opened channel to {}, last used: {}, channel: {}", connection.id, connection.lastUsed,
                          connection.channel);
                queue.remove(connection);
            }
            log.trace("Opened channel to {}, borrowed: {}, usage: {}", connection.id, connection.borrowed,
                      connection.usageCount);
            return createFunction.create(to, from, connection);
        });
    }

    public void close() {
        lock(() -> {
            log.info("Closing connection cache: {}", this);
            cache.values().forEach(conn -> {
                try {
                    conn.channel.shutdownNow();
                } catch (Throwable e) {
                    log.debug("Error closing {}", conn.id);
                }
            });
            cache.clear();
            queue.clear();
            return null;
        });
    }

    public int getIdleCount() {
        return lock(() -> queue.size());
    }

    public int getInUseCount() {
        return lock(() -> cache.size() - queue.size());
    }

    public int getOpenCount() {
        return lock(() -> cache.size());
    }

    public void release(ManagedServerConnection connection) {
        lock(() -> {
            if (connection.decrementBorrow()) {
                log.debug("Releasing connection: {}", connection.id);
                queue.add(connection);
                manageConnections();
            }
            return null;
        });

    }

    private boolean close(ManagedServerConnection connection) {
        if (connection.isCloseable()) {
            try {
                connection.channel.shutdownNow();
            } catch (Throwable t) {
                log.debug("Error closing {}", connection.id);
            }
            log.debug("{} is closed", connection.id);
            cache.remove(connection.id);
            return true;
        }
        return false;
    }

    private <T> T lock(Supplier<T> supplier) {
        final Lock l = lock.writeLock();
        l.lock();
        try {
            return supplier.get();
        } finally {
            l.unlock();
        }
    }

    private void manageConnections() {
        Iterator<ManagedServerConnection> connections = queue.iterator();
        while (connections.hasNext() && cache.size() > targetOpenAndIdle) {
            if (close(connections.next())) {
                connections.remove();
            }
        }
    }
}
