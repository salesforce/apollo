/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies.stats;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class DropWizardStatsPlugin extends RPCPlugin {
    ConcurrentMap<RPCContext, Long> activeRpcs = new ConcurrentHashMap<>();
    private final Map<Message, Histogram> methodTimings = new ConcurrentHashMap<>();
    private final Map<Message, Histogram> receivePayloads = new ConcurrentHashMap<>();
    private final MetricRegistry registry;
    private final Map<Message, Histogram> sendPayloads = new ConcurrentHashMap<>();

    public DropWizardStatsPlugin(MetricRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void clientReceiveResponse(RPCContext context) {
        long t = this.activeRpcs.remove(context);
        publish(context, System.nanoTime() - t);
        receivePayloads.computeIfAbsent(context.getMessage(),
                                        m -> registry.histogram(context.getMessage().getName()
                                                + " Client Response Received Payload Size"))
                       .update(getPayloadSize(context.getRequestPayload()));
    }

    @Override
    public void clientSendRequest(RPCContext context) {
        Long t = System.nanoTime();
        this.activeRpcs.put(context, t);
        sendPayloads.computeIfAbsent(context.getMessage(),
                                     m -> registry.histogram(context.getMessage().getName()
                                             + " Client Request Payload Size"))
                    .update(getPayloadSize(context.getRequestPayload()));
    }

    @Override
    public void serverReceiveRequest(RPCContext context) {
        this.activeRpcs.put(context, System.nanoTime());
        receivePayloads.computeIfAbsent(context.getMessage(),
                                        m -> registry.histogram(context.getMessage().getName()
                                                + " Server Receive Payload Size"))
                       .update(getPayloadSize(context.getRequestPayload()));
    }

    @Override
    public void serverSendResponse(RPCContext context) {
        long t = this.activeRpcs.remove(context);
        publish(context, System.nanoTime() - t);
        sendPayloads.computeIfAbsent(context.getMessage(),
                                     m -> registry.histogram(context.getMessage().getName()
                                             + " Server Response Sent Payload Size"))
                    .update(getPayloadSize(context.getResponsePayload()));
    }

    /**
     * Helper to get the size of an RPC payload.
     */
    private int getPayloadSize(List<ByteBuffer> payload) {
        if (payload == null) { return 0; }
        return payload.stream().mapToInt(e -> e.remaining()).sum();
    }

    private void publish(RPCContext context, long time) {
        Message message = context.getMessage();
        if (message == null)
            throw new IllegalArgumentException();
        Histogram h = methodTimings.get(context.getMessage());
        if (h == null) {
            h = registry.histogram(context.getMessage().getName() + " Timing");
            methodTimings.put(context.getMessage(), h);
        }
        h.update(time / 1_000_000);
    }

}
