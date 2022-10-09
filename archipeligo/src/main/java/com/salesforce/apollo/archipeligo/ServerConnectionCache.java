/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.MoreObjects;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

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
public class ServerConnectionCache<To extends Member> {

    public static class Builder<To extends Member> {
        private Clock                        clock   = Clock.systemUTC();
        private ServerConnectionFactory<To>  factory = null;
        private ServerConnectionCacheMetrics metrics;
        private Duration                     minIdle = Duration.ofMillis(100);
        private int                          target  = 0;

        public ServerConnectionCache<To> build() {
            return new ServerConnectionCache<>(factory, target, minIdle, clock, metrics);
        }

        public Clock getClock() {
            return clock;
        }

        public ServerConnectionFactory<To> getFactory() {
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

        public Builder<To> setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder<To> setFactory(ServerConnectionFactory<To> factory) {
            this.factory = factory;
            return this;
        }

        public Builder<To> setMetrics(ServerConnectionCacheMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder<To> setMinIdle(Duration minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public Builder<To> setTarget(int target) {
            this.target = target;
            return this;
        }
    }

    @FunctionalInterface
    public interface CreateClientCommunications<Client, To extends Member> {
        Client create(ReleasableManagedChannel<To> releasableManagedChannel);
    }

    public static class ManagedServerConnection<To extends Member> implements Comparable<ManagedServerConnection<To>> {
        private final AtomicInteger             borrowed   = new AtomicInteger();
        private final ManagedChannel            channel;
        private final Instant                   created;
        private volatile Instant                lastUsed;
        private final ServerConnectionCache<To> scc;
        private final To                        to;
        private final AtomicInteger             usageCount = new AtomicInteger();

        public ManagedServerConnection(To id, ManagedChannel channel, ServerConnectionCache<To> scc) {
            this.to = id;
            this.channel = channel;
            this.scc = scc;
            created = Instant.now(scc.clock);
            lastUsed = Instant.now(scc.clock);
        }

        @Override
        public int compareTo(ManagedServerConnection<To> o) {
            return Integer.compare(usageCount.get(), o.usageCount.get());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if ((obj == null) || (getClass() != obj.getClass()))
                return false;
            @SuppressWarnings("unchecked")
            ManagedServerConnection<To> other = (ManagedServerConnection<To>) obj;
            return to.equals(other.to);
        }

        public ManagedChannel getChannel() {
            return channel;
        }

        public To getTo() {
            return to;
        }

        @Override
        public int hashCode() {
            return to.hashCode();
        }

        public boolean isCloseable() {
            return lastUsed.plus(scc.minIdle).isBefore(Instant.now(scc.clock));
        }

        public void release() {
            scc.release(this);
        }

        private boolean decrementBorrow() {
            if (borrowed.decrementAndGet() == 0) {
                lastUsed = Instant.now(scc.clock);
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

    public interface ServerConnectionFactory<To extends Member> {
        ManagedChannel connectTo(To to);
    }

    static class ReleasableManagedChannel<To extends Member> extends ManagedChannel {

        private final Digest                      context;
        private final ManagedServerConnection<To> delegate;

        ReleasableManagedChannel(Digest context, ManagedServerConnection<To> delegate) {
            this.context = context;
            this.delegate = delegate;
        }

        @Override
        public String authority() {
            return delegate.channel.authority();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.channel.awaitTermination(timeout, unit);
        }

        @Override
        public void enterIdle() {
            delegate.channel.enterIdle();
        }

        @Override
        public ConnectivityState getState(boolean requestConnection) {
            return delegate.channel.getState(requestConnection);
        }

        public To getTo() {
            return delegate.to;
        }

        @Override
        public boolean isShutdown() {
            return delegate.channel.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.channel.isTerminated();
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                                                                             CallOptions callOptions) {
            return new SimpleForwardingClientCall<RequestT, ResponseT>(delegate.channel.newCall(methodDescriptor,
                                                                                                callOptions)) {
                @Override
                public void start(Listener<ResponseT> responseListener, Metadata headers) {
                    headers.put(Router.CONTEXT_METADATA_KEY, qb64(context));
                    super.start(responseListener, headers);
                }
            };
        }

        @Override
        public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
            delegate.channel.notifyWhenStateChanged(source, callback);
        }

        public void release() {
            delegate.release();
        }

        @Override
        public void resetConnectBackoff() {
            delegate.channel.resetConnectBackoff();
        }

        @Override
        public ManagedChannel shutdown() {
            return delegate.channel.shutdown();
        }

        @Override
        public ManagedChannel shutdownNow() {
            return delegate.channel.shutdownNow();
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("delegate", delegate).toString();
        }
    }

    private final static Logger log = LoggerFactory.getLogger(ServerConnectionCache.class);

    public static <To extends Member, From extends Member> Builder<To> newBuilder() {
        return new Builder<>();
    }

    private final Map<To, ManagedServerConnection<To>>       cache = new HashMap<>();
    private final Clock                                      clock;
    private final ServerConnectionFactory<To>                factory;
    private final ReentrantLock                              lock  = new ReentrantLock(true);
    private final ServerConnectionCacheMetrics               metrics;
    private final Duration                                   minIdle;
    private final PriorityQueue<ManagedServerConnection<To>> queue = new PriorityQueue<>();
    private final int                                        target;

    public ServerConnectionCache(ServerConnectionFactory<To> factory, int target, Duration minIdle, Clock clock,
                                 ServerConnectionCacheMetrics metrics) {
        this.factory = factory;
        this.target = target;
        this.minIdle = minIdle;
        this.clock = clock;
        this.metrics = metrics;
    }

    public <T> T borrow(Digest context, To to, CreateClientCommunications<T, To> createFunction) {
        return lock(() -> {
            if (cache.size() >= target) {
                log.debug("Cache target open connections exceeded: {}, opening from: {} to {}", target, to);
            }
            ManagedServerConnection<To> connection = cache.computeIfAbsent(to, member -> {
                ManagedServerConnection<To> conn = new ManagedServerConnection<To>(to, factory.connectTo(to), this);
                if (metrics != null) {
                    metrics.createConnection().inc();
                    metrics.openConnections().inc();
                }
                return conn;
            });
            if (connection == null) {
                log.warn("Failed to open channel to {}", to);
                if (metrics != null) {
                    metrics.failedOpenConnection().inc();
                    metrics.failedConnectionRate().mark();
                }
                return null;
            }
            if (connection.incrementBorrow()) {
                log.debug("Opened channel to {}, last used: {}", connection.to, connection.lastUsed);
                if (metrics != null) {
                    metrics.borrowRate().mark();
                }
                queue.remove(connection);
            }
            log.trace("Opened channel to {}, borrowed: {}, usage: {}", connection.to, connection.borrowed,
                      connection.usageCount);
            return createFunction.create(new ReleasableManagedChannel<To>(context, connection));
        });
    }

    public void close() {
        lock(() -> {
            log.info("Closing connection cache: {}", this);
            for (ManagedServerConnection<To> conn : new ArrayList<>(cache.values())) {
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

    public void release(ManagedServerConnection<To> connection) {
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

    private boolean close(ManagedServerConnection<To> connection) {
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
        Iterator<ManagedServerConnection<To>> connections = queue.iterator();
        while (connections.hasNext() && cache.size() > target) {
            if (close(connections.next())) {
                connections.remove();
            }
        }
    }
}
