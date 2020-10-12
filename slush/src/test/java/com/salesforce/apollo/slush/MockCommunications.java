/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.fireflies.Participant;

/**
 * @author hal.hildebrand
 * @since 220
 */
@SuppressWarnings("rawtypes")
public class MockCommunications<Protocol extends AbstractProtocol<?, ?>> implements Communications {
    protected static class TimeSink {
        final ScheduledFuture<?> futureSailor;
        final Sink<?>            sink;

        TimeSink(Sink<?> sink, ScheduledFuture<?> futureSailor) {
            this.sink = sink;
            this.futureSailor = futureSailor;
        }
    }

    private static Logger log = LoggerFactory.getLogger(MockCommunications.class);

    protected final Participant                                    address;
    protected final ConcurrentMap<UUID, TimeSink>             outstanding = new ConcurrentHashMap<>();
    protected final ConcurrentMap<Participant, MockCommunications> peers       = new ConcurrentHashMap<>();
    protected final ScheduledExecutorService                  scheduler;

    private Protocol protocol;

    public MockCommunications(Participant address, ScheduledExecutorService scheduler) {
        this.address = address;
        this.scheduler = scheduler;
    }

    public Communications addPeer(MockCommunications mock) {
        peers.putIfAbsent(mock.address(), mock);
        return this;
    }

    @Override
    public Participant address() {
        return address;
    }

    @Override
    public <T> Communications await(Sink<T> sink, long timeout, TimeUnit unit) {
        TimeSink timeSink = new TimeSink(sink, scheduler.schedule(() -> {
            if (outstanding.remove(sink.getId()) != null) {
                log.debug("{} timing out {}", address, sink.getId());
                scheduler.execute(() -> {
                    try {
                        sink.onTimeout();
                    } catch (Throwable t) {
                        log.error(address + " error in timeout of " + sink.getId(), t);
                    }
                });
            }
        }, timeout, unit));
        outstanding.put(sink.getId(), timeSink);
        return this;
    }

    @Override
    public Communications cancel(UUID id) {
        TimeSink timeSink = outstanding.remove(id);
        if (timeSink != null) {
            timeSink.futureSailor.cancel(true);
        }
        return this;
    }

    @Override
    public Set<Participant> getPopulation() {
        return peers.keySet();
    }

    public Protocol getProtocol() {
        return protocol;
    }

    @Override
    public <T> Communications query(Supplier<T> message, Collection<Participant> to, UUID sink) {
        scheduler.execute(() -> {
            try {
                to.stream().map(peer -> peers.get(peer)).filter(c -> c != null).forEach(coms -> {
                    try {
                        log.debug("{} query ({}) to {}", address, message.get(), coms.address());
                        @SuppressWarnings("unchecked")
                        T response = (T) coms.getProtocol().query(message.get());
                        log.debug("{} response {} from {}", address, response, coms.address());
                        TimeSink timeSink = outstanding.get(sink);
                        if (timeSink != null) {
                            @SuppressWarnings("unchecked")
                            Sink<T> ts = (Sink<T>) timeSink.sink;
                            ts.consume(coms.address(), response);
                        }
                    } catch (Throwable t) {
                        log.error(address + " error in sending to " + coms.address(), t);
                    }
                });
            } catch (Throwable t) {
                log.error(address + " error in sending to " + to.stream().map(a -> a).collect(Collectors.toList()), t);
            }
        });
        return this;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

}
