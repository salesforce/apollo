package com.salesforce.apollo.fireflies;

import com.google.common.collect.Sets;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.fireflies.proto.Ping;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author hal.hildebrand
 **/
public class Monitor {
    private final static Logger                            log       = LoggerFactory.getLogger(Monitor.class);
    private final        View                              view;
    private final        Map<View.Participant, State>      monitored = new HashMap<>();
    private final        PhiAccrualFailureDetector.Builder factory;
    private final        Lock                              cycleLock = new ReentrantLock();
    private final        Duration                          timeout;

    public Monitor(View view, PhiAccrualFailureDetector.Builder factory, Duration timeout) {
        this.view = view;
        this.factory = factory;
        this.timeout = timeout;
    }

    /**
     * Perform one cycle of the monitoring
     */
    public void cycle() {
        cycleLock.lock();
        try {
            resynchronize();
            monitorOnce();
            var now = System.currentTimeMillis();
            for (var e : monitored.entrySet()) {
                if (!e.getValue().fd.isAvailable(now)) {
                    view.accuse(e.getKey(), e.getValue().ring, new Phailure(e.getKey().getId()));
                }
            }
        } finally {
            cycleLock.unlock();
        }
    }

    private void handleSre(int ring, StatusRuntimeException sre, View.Participant p) {
        switch (sre.getStatus().getCode()) {
        case PERMISSION_DENIED:
            log.trace("Rejected ping: {} from: {} on: {}", sre.getStatus(), p.getId(), view.getNodeId());
            view.accuse(p, ring, sre);
            break;
        case FAILED_PRECONDITION:
            log.trace("Failed ping: {}  from: {} on: {}", sre.getStatus(), p.getId(), view.getNodeId());
            break;
        case RESOURCE_EXHAUSTED:
            log.trace("Resource exhausted for ping: {}  from: {} on: {}", sre.getStatus(), p.getId(), view.getNodeId());
            break;
        case CANCELLED:
            log.trace("Communication cancelled for ping  from: {} on: {}", p.getId(), view.getNodeId());
            break;
        case UNAVAILABLE:
            log.trace("Communication unavailable for ping  from: {} on: {}", p.getId(), view.getNodeId());
            view.accuse(p, ring, sre);
            break;
        default:
            log.debug("Error pinging: {}  from: {} on: {}", sre.getStatus(), p.getId(), view.getNodeId());
            view.accuse(p, ring, sre);
            break;
        }
    }

    private void monitorOnce() {
        for (var e : monitored.entrySet()) {
            try (var link = view.comm.connect(e.getKey())) {
                var l = link.ping(Ping.newBuilder().setRing(e.getValue().ring).build(), timeout);
                l.addListener(() -> {
                    try {
                        l.get();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof StatusRuntimeException sre) {
                            handleSre(e.getValue().ring, sre, e.getKey());
                        }
                    }
                    e.getValue().fd.heartbeat();
                }, Runnable::run);
            } catch (StatusRuntimeException sre) {
                handleSre(e.getValue().ring, sre, e.getKey());
            } catch (Throwable ex) {
                log.debug("Exception pinging: {}  from: {} on: {}", ex, e.getKey().getId(), view.getNodeId());
                view.accuse(e.getKey(), e.getValue().ring, ex);
            }
        }
    }

    private void resynchronize() {
        var context = view.getContext();
        var successors = context.successors(view.getNodeId(), context::isActive, view.getNode())
                                .stream()
                                .collect(Collectors.toMap(Context.iteration::m, Context.iteration::ring));

        // remove all previously monitored not in the successor set
        for (var m : Sets.difference(monitored.keySet(), successors.keySet())) {
            monitored.remove(m);
        }
        // Update rings on any previous members
        for (var m : Sets.intersection(monitored.keySet(), successors.keySet())) {
            var state = monitored.get(m);
            var i = successors.get(m);
            if (!i.equals(state.ring)) {
                monitored.put(m, new State(i, state.fd));
            }
        }
        // Add newly monitored members
        for (var m : Sets.difference(successors.keySet(), monitored.keySet())) {
            monitored.put(m, new State(successors.get(m), factory.build()));
        }
    }

    private record State(Integer ring, PhiAccrualFailureDetector fd) {
    }

    public class Phailure extends RuntimeException {
        private final Digest id;

        public Phailure(Digest id) {
            super("Failure detected of: %s on: %s".formatted(id, view.getNodeId()));
            this.id = id;
        }

        public Digest getId() {
            return id;
        }
    }
}
