/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import io.grpc.ClientInterceptor;
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
public class ServerConnectionCache<To, From> {

    public static class Builder<To, From> {
        private Clock                             clock   = Clock.systemUTC();
        private ServerConnectionFactory<To, From> factory = null;
        private ServerConnectionCacheMetrics      metrics;
        private Duration                          minIdle = Duration.ofMillis(100);
        private int                               target  = 0;

        public ServerConnectionCache<To, From> build(ClientInterceptor clientInterceptor) {
            return new ServerConnectionCache<>(factory, target, minIdle, clock, metrics, clientInterceptor);
        }

        public Clock getClock() {
            return clock;
        }

        public ServerConnectionFactory<To, From> getFactory() {
            return factory;
        }

        public ServerConnectionCacheMetrics getMetrics() {
            return metrics;
        }

        public Duration getMinIdle() {
            return minIdle;
        }

        public int getTarget() {
            return target;
        }

        public Builder<To, From> setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder<To, From> setFactory(ServerConnectionFactory<To, From> factory) {
            this.factory = factory;
            return this;
        }

        public Builder<To, From> setMetrics(ServerConnectionCacheMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder<To, From> setMinIdle(Duration minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public Builder<To, From> setTarget(int target) {
            this.target = target;
            return this;
        }
    }

    @FunctionalInterface
    public static interface CreateClientCommunications<T, To, From> {
        T create(To to, From from, ServerConnectionCache<To, From>.ManagedServerConnection connection);
    }

    public class ManagedServerConnection implements Comparable<ManagedServerConnection> {
        final ManagedChannel        channel;
        final To                    to;
        private final AtomicInteger borrowed   = new AtomicInteger();
        private final Instant       created    = Instant.now(clock);
        private volatile Instant    lastUsed   = Instant.now(clock);
        private final AtomicInteger usageCount = new AtomicInteger();

        public ManagedServerConnection(To id, ManagedChannel channel) {
            this.to = id;
            this.channel = channel;
        }

        @Override
        public int compareTo(ManagedServerConnection o) {
            return Integer.compare(usageCount.get(), o.usageCount.get());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if ((obj == null) || (getClass() != obj.getClass()))
                return false;
            @SuppressWarnings("unchecked")
            ManagedServerConnection other = (ManagedServerConnection) obj;
            return to.equals(other.to);
        }

        @Override
        public int hashCode() {
            return to.hashCode();
        }

        public boolean isCloseable() {
            return lastUsed.plus(minIdle).isBefore(Instant.now(clock));
        }

        public void release() {
            ServerConnectionCache.this.release(this);
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

    public static interface ServerConnectionCacheMetrics {

        Meter borrowRate();

        Timer channelOpenDuration();

        Meter closeConnectionRate();

        Counter createConnection();

        Meter failedConnectionRate();

        Counter failedOpenConnection();

        Counter openConnections();

        Meter releaseRate();

    }

    public static interface ServerConnectionFactory<To, From> {
        ManagedChannel connectTo(To to, From from, ClientInterceptor interceptor);
    }

    private final static Logger log = LoggerFactory.getLogger(ServerConnectionCache.class);

    public static <To, From> Builder<To, From> newBuilder() {
        return new Builder<>();
    }

    private final Map<To, ManagedServerConnection>       cache = new HashMap<>();
    private final ClientInterceptor                      clientInterceptor;
    private final Clock                                  clock;
    private final ServerConnectionFactory<To, From>      factory;
    private final ReentrantLock                          lock  = new ReentrantLock(true);
    private final ServerConnectionCacheMetrics           metrics;
    private final Duration                               minIdle;
    private final PriorityQueue<ManagedServerConnection> queue = new PriorityQueue<>();
    private final int                                    target;

    public ServerConnectionCache(ServerConnectionFactory<To, From> factory, int target, Duration minIdle, Clock clock,
                                 ServerConnectionCacheMetrics metrics, ClientInterceptor clientInterceptor) {
        this.factory = factory;
        this.target = target;
        this.minIdle = minIdle;
        this.clock = clock;
        this.metrics = metrics;
        this.clientInterceptor = clientInterceptor;
    }

    public <T> T borrow(To to, From from, CreateClientCommunications<T, To, From> createFunction) {
        return lock(() -> {
            if (cache.size() >= target) {
                log.debug("Cache target open connections exceeded: {}, opening from: {} to {}", target, from, to);
            }
            ManagedServerConnection connection = cache.computeIfAbsent(to, member -> {
                ManagedServerConnection conn = new ManagedServerConnection(to, factory.connectTo(to, from,
                                                                                                 clientInterceptor));
                if (metrics != null) {
                    metrics.createConnection().inc();
                    metrics.openConnections().inc();
                }
                return conn;
            });
            if (connection == null) {
                log.warn("Failed to open channel to {} from {}", to, from);
                if (metrics != null) {
                    metrics.failedOpenConnection().inc();
                    metrics.failedConnectionRate().mark();
                }
                return null;
            }
            if (connection.incrementBorrow()) {
                log.debug("Opened channel to {}, last used: {}, from: {}", connection.to, connection.lastUsed, from);
                if (metrics != null) {
                    metrics.borrowRate().mark();
                }
                queue.remove(connection);
            }
            log.trace("Opened channel to {}, borrowed: {}, usage: {}", connection.to, connection.borrowed,
                      connection.usageCount);
            return createFunction.create(to, from, connection);
        });
    }

    public void close() {
        lock(() -> {
            log.info("Closing connection cache: {}", this);
            for (ManagedServerConnection conn : new ArrayList<>(cache.values())) {
                try {
                    conn.channel.shutdownNow();
                    if (metrics != null) {
                        metrics.channelOpenDuration().update(Duration.between(conn.created, Instant.now(clock)));
                        metrics.openConnections().dec();
                    }
                } catch (Throwable e) {
                    log.debug("Error closing {}", conn.to);
                }
            }
            cache.clear();
            queue.clear();
            return null;
        });
    }

    public void release(ManagedServerConnection connection) {
        lock(() -> {
            if (connection.decrementBorrow()) {
                log.debug("Releasing connection: {}", connection.to);
                queue.add(connection);
                if (metrics != null) {
                    metrics.releaseRate().mark();
                }
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
                log.debug("Error closing {}", connection.to);
            }
            log.debug("{} is closed", connection.to);
            cache.remove(connection.to);
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
//        log.info("Managing connections: " + cache.size() + " idle: " + queue.size());
        Iterator<ManagedServerConnection> connections = queue.iterator();
        while (connections.hasNext() && cache.size() > target) {
            if (close(connections.next())) {
                connections.remove();
            }
        }
    }
}
