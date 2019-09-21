/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.apollo.fireflies.stats;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;

import com.salesforce.apollo.fireflies.stats.Histogram.Segmenter;
import com.salesforce.apollo.fireflies.stats.Stopwatch.Ticks;

/**
 * Collects count and latency statistics about RPC calls. Keeps data for every method. Can be added to a Requestor
 * (client) or Responder (server). This uses milliseconds as the standard unit of measure throughout the class, stored
 * in floats.
 */
public class StatsPlugin extends RPCPlugin {
    /** Static declaration of histogram buckets. */
    static final Segmenter<String, Float> LATENCY_SEGMENTER = new Histogram.TreeMapSegmenter<Float>(new TreeSet<Float>(Arrays.asList(
                                                                                                                                     0f,
                                                                                                                                     25f,
                                                                                                                                     50f,
                                                                                                                                     75f,
                                                                                                                                     100f,
                                                                                                                                     200f,
                                                                                                                                     300f,
                                                                                                                                     500f,
                                                                                                                                     750f,
                                                                                                                                     1000f, // 1
                                                                                                                                            // second
                                                                                                                                     2000f,
                                                                                                                                     5000f,
                                                                                                                                     10000f,
                                                                                                                                     60000f, // 1
                                                                                                                                             // minute
                                                                                                                                     600000f)));

    static final Segmenter<String, Integer> PAYLOAD_SEGMENTER = new Histogram.TreeMapSegmenter<Integer>(new TreeSet<Integer>(Arrays.asList(
                                                                                                                                           0,
                                                                                                                                           25,
                                                                                                                                           50,
                                                                                                                                           75,
                                                                                                                                           100,
                                                                                                                                           200,
                                                                                                                                           300,
                                                                                                                                           500,
                                                                                                                                           750,
                                                                                                                                           1000, // 1
                                                                                                                                                 // k
                                                                                                                                           2000,
                                                                                                                                           5000,
                                                                                                                                           10000,
                                                                                                                                           50000,
                                                                                                                                           100000)));

    /** Converts nanoseconds to milliseconds. */
    static float nanosToMillis(long elapsedNanos) {
        return elapsedNanos / 1000000.0f;
    }

    /** How long I've been alive */
    public Date startupTime = new Date();

    /** RPCs in flight. */
    ConcurrentMap<RPCContext, Stopwatch> activeRpcs = new ConcurrentHashMap<RPCContext, Stopwatch>();

    /**
     * Per-method histograms. Must be accessed while holding a lock.
     */
    Map<Message, FloatHistogram<?>> methodTimings = new HashMap<Message, FloatHistogram<?>>();
    Map<Message, IntegerHistogram<?>> receivePayloads = new HashMap<Message, IntegerHistogram<?>>();

    Map<Message, IntegerHistogram<?>> sendPayloads = new HashMap<Message, IntegerHistogram<?>>();

    private Segmenter<?, Float> floatSegmenter;
    private Segmenter<?, Integer> integerSegmenter;

    private Ticks ticks;

    /**
     * Construct a plugin with default (system) ticks, and default histogram segmentation.
     */
    public StatsPlugin() {
        this(Stopwatch.SYSTEM_TICKS, LATENCY_SEGMENTER, PAYLOAD_SEGMENTER);
    }

    /** Construct a plugin with custom Ticks and Segmenter implementations. */
    StatsPlugin(Ticks ticks, Segmenter<?, Float> floatSegmenter,
            Segmenter<?, Integer> integerSegmenter) {
        this.floatSegmenter = floatSegmenter;
        this.integerSegmenter = integerSegmenter;
        this.ticks = ticks;
    }

    @Override
    public void clientReceiveResponse(RPCContext context) {
        Stopwatch t = this.activeRpcs.remove(context);
        t.stop();
        publish(context, t);

        synchronized (receivePayloads) {
            IntegerHistogram<?> h = receivePayloads.get(context.getMessage());
            if (h == null) {
                h = createNewIntegerHistogram();
                receivePayloads.put(context.getMessage(), h);
            }
            h.add(getPayloadSize(context.getRequestPayload()));
        }
    }

    @Override
    public void clientSendRequest(RPCContext context) {
        Stopwatch t = new Stopwatch(ticks);
        t.start();
        this.activeRpcs.put(context, t);

        synchronized (sendPayloads) {
            IntegerHistogram<?> h = sendPayloads.get(context.getMessage());
            if (h == null) {
                h = createNewIntegerHistogram();
                sendPayloads.put(context.getMessage(), h);
            }
            h.add(getPayloadSize(context.getRequestPayload()));
        }
    }

    @Override
    public void serverReceiveRequest(RPCContext context) {
        Stopwatch t = new Stopwatch(ticks);
        t.start();
        this.activeRpcs.put(context, t);

        synchronized (receivePayloads) {
            IntegerHistogram<?> h = receivePayloads.get(context.getMessage());
            if (h == null) {
                h = createNewIntegerHistogram();
                receivePayloads.put(context.getMessage(), h);
            }
            h.add(getPayloadSize(context.getRequestPayload()));
        }
    }

    @Override
    public void serverSendResponse(RPCContext context) {
        Stopwatch t = this.activeRpcs.remove(context);
        t.stop();
        publish(context, t);

        synchronized (sendPayloads) {
            IntegerHistogram<?> h = sendPayloads.get(context.getMessage());
            if (h == null) {
                h = createNewIntegerHistogram();
                sendPayloads.put(context.getMessage(), h);
            }
            h.add(getPayloadSize(context.getResponsePayload()));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private FloatHistogram<?> createNewFloatHistogram() {
        return new FloatHistogram(floatSegmenter);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private IntegerHistogram<?> createNewIntegerHistogram() {
        return new IntegerHistogram(integerSegmenter);
    }

    /**
     * Helper to get the size of an RPC payload.
     */
    private int getPayloadSize(List<ByteBuffer> payload) {
        if (payload == null) { return 0; }

        int size = 0;
        for (ByteBuffer bb : payload) {
            size = size + bb.limit();
        }

        return size;
    }

    /** Adds timing to the histograms. */
    private void publish(RPCContext context, Stopwatch t) {
        Message message = context.getMessage();
        if (message == null) throw new IllegalArgumentException();
        synchronized (methodTimings) {
            FloatHistogram<?> h = methodTimings.get(context.getMessage());
            if (h == null) {
                h = createNewFloatHistogram();
                methodTimings.put(context.getMessage(), h);
            }
            h.add(nanosToMillis(t.elapsedNanos()));
        }
    }

    public class RenderableMessage { // Velocity brakes if not public
        public String name;
        public int numCalls;
        public ArrayList<HashMap<String, String>> charts;

        public RenderableMessage(String name) {
            this.name = name;
            this.charts = new ArrayList<>();
        }

        public ArrayList<HashMap<String, String>> getCharts() {
            return this.charts;
        }

        public String getname() {
            return this.name;
        }

        public int getNumCalls() {
            return this.numCalls;
        }

        @Override
        public String toString() {
            return "[name=" + name + ", numCalls=" + numCalls + ", charts=" + charts + "]";
        }
    }

    public String report() {
        StringBuffer buff = new StringBuffer();
        report(buff);
        return buff.toString();
    }

    public void report(StringBuffer writer) {

        writer.append("Avro RPC Stats").append("\n");

        ArrayList<String> rpcs = new ArrayList<>(); // in flight rpcs

        ArrayList<RenderableMessage> messages = new ArrayList<>();

        for (Entry<RPCContext, Stopwatch> rpc : activeRpcs.entrySet()) {
            rpcs.add(renderActiveRpc(rpc.getKey(), rpc.getValue()));
        }

        // Get set of all seen messages
        Set<Message> keys = null;
        synchronized (methodTimings) {
            keys = methodTimings.keySet();

            for (Message m : keys) {
                messages.add(renderMethod(m));
            }
        }

        writer.append("inFlightRpcs").append("\n");
        rpcs.forEach(e -> writer.append(e).append("\n"));
        writer.append("messages").append("\n");
        messages.forEach(e -> writer.append(e.toString()).append("\n"));
    }

    private String renderActiveRpc(RPCContext rpc, Stopwatch stopwatch) {
        String out = new String();
        out += rpc.getMessage().getName() + ": " + formatMillis(StatsPlugin.nanosToMillis(stopwatch.elapsedNanos()));
        return out;
    }

    private RenderableMessage renderMethod(Message message) {
        RenderableMessage out = new RenderableMessage(message.getName());

        synchronized (methodTimings) {
            FloatHistogram<?> hist = methodTimings.get(message);
            out.numCalls = hist.getCount();

            HashMap<String, String> latencyBar = new HashMap<>();
            // Fill in chart attributes for velocity
            latencyBar.put("type", "bar");
            latencyBar.put("title", "All-Time Latency");
            latencyBar.put("units", "ms");
            latencyBar.put("numCalls", Integer.toString(hist.getCount()));
            latencyBar.put("avg", Float.toString(hist.getMean()));
            latencyBar.put("stdDev", Float.toString(hist.getUnbiasedStdDev()));
            latencyBar.put("labelStr", Arrays.toString(hist.getSegmenter().getBoundaryLabels().toArray()));
            latencyBar.put("boundaryStr",
                           Arrays.toString(escapeStringArray(hist.getSegmenter().getBucketLabels()).toArray()));
            latencyBar.put("dataStr", Arrays.toString(hist.getHistogram()));
            out.charts.add(latencyBar);

            HashMap<String, String> latencyDot = new HashMap<>();
            latencyDot.put("title", "Latency");
            latencyDot.put("type", "dot");
            latencyDot.put("dataStr", Arrays.toString(hist.getRecentAdditions().toArray()));
            out.charts.add(latencyDot);
        }

        synchronized (sendPayloads) {
            IntegerHistogram<?> hist = sendPayloads.get(message);
            HashMap<String, String> latencyBar = new HashMap<>();
            // Fill in chart attributes for velocity
            latencyBar.put("type", "bar");
            latencyBar.put("title", "All-Time Send Payload");
            latencyBar.put("units", "ms");
            latencyBar.put("numCalls", Integer.toString(hist.getCount()));
            latencyBar.put("avg", Float.toString(hist.getMean()));
            latencyBar.put("stdDev", Float.toString(hist.getUnbiasedStdDev()));
            latencyBar.put("labelStr", Arrays.toString(hist.getSegmenter().getBoundaryLabels().toArray()));
            latencyBar.put("boundaryStr",
                           Arrays.toString(escapeStringArray(hist.getSegmenter().getBucketLabels()).toArray()));
            latencyBar.put("dataStr", Arrays.toString(hist.getHistogram()));
            out.charts.add(latencyBar);

            HashMap<String, String> latencyDot = new HashMap<>();
            latencyDot.put("title", "Send Payload");
            latencyDot.put("type", "dot");
            latencyDot.put("dataStr", Arrays.toString(hist.getRecentAdditions().toArray()));
            out.charts.add(latencyDot);
        }

        synchronized (receivePayloads) {
            IntegerHistogram<?> hist = receivePayloads.get(message);
            HashMap<String, String> latencyBar = new HashMap<>();
            // Fill in chart attributes for velocity
            latencyBar.put("type", "bar");
            latencyBar.put("title", "All-Time Receive Payload");
            latencyBar.put("units", "ms");
            latencyBar.put("numCalls", Integer.toString(hist.getCount()));
            latencyBar.put("avg", Float.toString(hist.getMean()));
            latencyBar.put("stdDev", Float.toString(hist.getUnbiasedStdDev()));
            latencyBar.put("labelStr", Arrays.toString(hist.getSegmenter().getBoundaryLabels().toArray()));
            latencyBar.put("boundaryStr",
                           Arrays.toString(escapeStringArray(hist.getSegmenter().getBucketLabels()).toArray()));
            latencyBar.put("dataStr", Arrays.toString(hist.getHistogram()));
            out.charts.add(latencyBar);

            HashMap<String, String> latencyDot = new HashMap<>();
            latencyDot.put("title", "Recv Payload");
            latencyDot.put("type", "dot");
            latencyDot.put("dataStr", Arrays.toString(hist.getRecentAdditions().toArray()));
            out.charts.add(latencyDot);
        }

        return out;
    }

    private CharSequence formatMillis(float millis) {
        return String.format("%.0fms", millis);
    }

    protected static List<String> escapeStringArray(List<String> input) {
        for (int i = 0; i < input.size(); i++) {
            input.set(i, "\"" + input.get(i).replace("\"", "\\\"") + "\"");
        }
        return input;
    }
}
