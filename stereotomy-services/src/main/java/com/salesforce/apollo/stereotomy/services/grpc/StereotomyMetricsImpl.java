/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.BandwidthMetricsImpl;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyMetricsImpl extends BandwidthMetricsImpl implements StereotomyMetrics {
    private final Timer appendEventsClient;
    private final Timer appendEventsService;
    private final Timer appendKERLClient;
    private final Timer appendKERLService;
    private final Timer appendWithAttachmentsClient;
    private final Timer appendWithAttachmentsService;
    private final Timer bindClient;
    private final Timer bindService;
    private final Timer getAttachmentClient;
    private final Timer getAttachmentService;
    private final Timer getKERLClient;
    private final Timer getKERLService;
    private final Timer getKeyEventClient;
    private final Timer getKeyEventCoordsClient;
    private final Timer getKeyEventCoordsService;
    private final Timer getKeyEventService;
    private final Timer getKeyStateClient;
    private final Timer getKeyStateCoordsClient;
    private final Timer getKeyStateCoordsService;
    private final Timer getKeyStateService;
    private final Meter inboundAppendEventsRequest;
    private final Meter inboundAppendEventsResponse;
    private final Meter inboundAppendKERLRequest;
    private final Meter inboundAppendKERLResponse;
    private final Meter inboundAppendWithAttachmentsRequest;
    private final Meter inboundAppendWithAttachmentsResponse;
    private final Meter inboundBindRequest;
    private final Meter inboundGetAttachmentRequest;
    private final Meter inboundGetAttachmentResponse;
    private final Meter inboundGetKERLRequest;
    private final Meter inboundGetKERLResponse;
    private final Meter inboundGetKeyEventCoordsRequest;
    private final Meter inboundGetKeyEventCoordsResponse;
    private final Meter inboundGetKeyEventRequest;
    private final Meter inboundGetKeyEventResponse;
    private final Meter inboundGetKeyStateCoordsRequest;
    private final Meter inboundGetKeyStateCoordsResponse;
    private final Meter inboundGetKeyStateRequest;
    private final Meter inboundGetKeyStateResponse;
    private final Meter inboundLookupRequest;
    private final Meter inboundLookupResponse;
    private final Meter inboundPublishAttachmentsRequest;
    private final Meter inboundPublishEventsRequest;
    private final Meter inboundPublishEventsResponse;
    private final Meter inboundPublishKERLRequest;
    private final Meter inboundPublishKERLResponse;
    private final Meter inboundUnbindRequest;
    private final Meter inboundValidatorRequest;
    private final Timer lookupClient;
    private final Timer lookupService;
    private final Meter outboudUnbindRequest;
    private final Meter outboundAppendEventsRequest;
    private final Meter outboundAppendEventsResponse;
    private final Meter outboundAppendKERLRequest;
    private final Meter outboundAppendKERLResponse;
    private final Meter outboundAppendWithAttachmentsRequest;
    private final Meter outboundAppendWithAttachmentsResponse;
    private final Meter outboundBindRequest;
    private final Meter outboundGetAttachmentRequest;
    private final Meter outboundGetAttachmentResponse;
    private final Meter outboundGetKERLRequest;
    private final Meter outboundGetKERLResponse;
    private final Meter outboundGetKeyEventCoordsRequest;
    private final Meter outboundGetKeyEventCoordsResponse;
    private final Meter outboundGetKeyEventRequest;
    private final Meter outboundGetKeyEventResponse;
    private final Meter outboundGetKeyStateCoordsRequest;
    private final Meter outboundGetKeyStateCoordsResponse;
    private final Meter outboundGetKeyStateRequest;
    private final Meter outboundGetKeyStateResponse;
    private final Meter outboundLookupRequest;
    private final Meter outboundLookupResponse;
    private final Meter outboundPublishAttachmentsRequest;
    private final Meter outboundPublishEventsRequest;
    private final Meter outboundPublishEventsResponse;
    private final Meter outboundPublishKERLRequest;
    private final Meter outboundPublishKERLResponse;
    private final Meter outboundValidatorRequest;
    private final Timer publishAttachmentsClient;
    private final Timer publishAttachmentsService;
    private final Timer publishEventsClient;
    private final Timer publishEventsService;
    private final Timer publishKERLClient;
    private final Timer publishKERLService;
    private final Timer unbindClient;
    private final Timer unbindService;
    private final Timer validatorClient;
    private final Timer validatorService;

    public StereotomyMetricsImpl(Digest context, MetricRegistry registry) {
        super(registry);
        this.appendEventsClient = registry.timer(name(context.shortString(), "append.events.client.duration"));
        this.appendEventsService = registry.timer(name(context.shortString(), "append.events.server.duration"));
        this.appendKERLClient = registry.timer(name(context.shortString(), "append.kerl.client.duration"));
        this.appendKERLService = registry.timer(name(context.shortString(), "append.kerl.service.duration"));
        this.appendWithAttachmentsClient = registry.timer(name(context.shortString(),
                                                               "append.with.attachments.client.duration"));
        this.appendWithAttachmentsService = registry.timer(name(context.shortString(),
                                                                "append.with.attachments.duration"));
        this.bindClient = registry.timer(name(context.shortString(), "bind.client.duration"));
        this.bindService = registry.timer(name(context.shortString(), "bind.service.duration"));
        this.getAttachmentClient = registry.timer(name(context.shortString(), "get.attachment.client.duration"));
        this.getAttachmentService = registry.timer(name(context.shortString(), "get.attachment.service.duration"));
        this.getKERLClient = registry.timer(name(context.shortString(), "get.kerl.client.duration"));
        this.getKERLService = registry.timer(name(context.shortString(), "get.kerl.service.duration"));
        this.getKeyEventClient = registry.timer(name(context.shortString(), "get.key.event.client.duration"));
        this.getKeyEventCoordsClient = registry.timer(name(context.shortString(),
                                                           "get.key.event.coords.client.duration"));
        this.getKeyEventCoordsService = registry.timer(name(context.shortString(), "get.key.event.service.duration"));
        this.getKeyEventService = registry.timer(name(context.shortString(), "get.key.event.service.duration"));
        this.getKeyStateClient = registry.timer(name(context.shortString(), "get.key.state.client.duration"));
        this.getKeyStateCoordsClient = registry.timer(name(context.shortString(),
                                                           "get.key.state.coords.client.duration"));
        this.getKeyStateCoordsService = registry.timer(name(context.shortString(),
                                                            "get.key.state.coords.service.duration"));
        this.getKeyStateService = registry.timer(name(context.shortString(), "get.key.state.service.duration"));
        this.inboundAppendEventsRequest = registry.meter(name(context.shortString(), "inbound.append.events.request"));
        this.inboundAppendEventsResponse = registry.meter(name(context.shortString(),
                                                               "inbound.append.events.response"));
        this.inboundAppendKERLRequest = registry.meter(name(context.shortString(), "inbound.append.kerl.request"));
        this.inboundAppendKERLResponse = registry.meter(name(context.shortString(), "inbound.append.kerl.response"));
        this.inboundAppendWithAttachmentsRequest = registry.meter(name(context.shortString(),
                                                                       "inbound.append.with.attachments.request"));
        this.inboundAppendWithAttachmentsResponse = registry.meter(name(context.shortString(),
                                                                        "inbound.append.with.attachments.response"));
        this.inboundBindRequest = registry.meter(name(context.shortString(), "inbound.bind.request"));
        this.inboundGetAttachmentRequest = registry.meter(name(context.shortString(),
                                                               "inbound.get.attachment.request"));
        this.inboundGetAttachmentResponse = registry.meter(name(context.shortString(),
                                                                "inbound.get.attachment.response"));
        this.inboundGetKERLRequest = registry.meter(name(context.shortString(), "inbound.get.kerl.request"));
        this.inboundGetKERLResponse = registry.meter(name(context.shortString(), "inbound.get.kerl.response"));
        this.inboundGetKeyEventCoordsRequest = registry.meter(name(context.shortString(),
                                                                   "inbound.get.key.event.coords.request"));
        this.inboundGetKeyEventCoordsResponse = registry.meter(name(context.shortString(),
                                                                    "inbound.get.key.event.coords.response"));
        this.inboundGetKeyEventRequest = registry.meter(name(context.shortString(), "inbound.get.key.event.request"));
        this.inboundGetKeyEventResponse = registry.meter(name(context.shortString(), "inbound.get.key.event.response"));
        this.inboundGetKeyStateCoordsRequest = registry.meter(name(context.shortString(),
                                                                   "inbound.get.key.state.coords.request"));
        this.inboundGetKeyStateCoordsResponse = registry.meter(name(context.shortString(),
                                                                    "inbound.get.key.state.coords.response"));
        this.inboundGetKeyStateRequest = registry.meter(name(context.shortString(), "inbound.get.key.state.request"));
        this.inboundGetKeyStateResponse = registry.meter(name(context.shortString(), "inbound.get.key.state.response"));
        this.inboundPublishAttachmentsRequest = registry.meter(name(context.shortString(),
                                                                    "inbound.publish.attachments.request"));
        this.inboundPublishEventsRequest = registry.meter(name(context.shortString(),
                                                               "inbound.publish.events.request"));
        this.inboundPublishEventsResponse = registry.meter(name(context.shortString(),
                                                                "inbound.publish.events.request"));
        this.inboundPublishKERLRequest = registry.meter(name(context.shortString(), "inbound.publish.kerl.request"));
        this.inboundPublishKERLResponse = registry.meter(name(context.shortString(), "inbound.publish.kerl.response"));
        this.inboundUnbindRequest = registry.meter(name(context.shortString(), "inbound.unbind.request"));
        this.inboundLookupRequest = registry.meter(name(context.shortString(), "inbound.lookup.request"));
        this.inboundLookupResponse = registry.meter(name(context.shortString(), "inbound.lookup.response"));
        this.inboundValidatorRequest = registry.meter(name(context.shortString(), "inbound.validator.request"));
        this.lookupClient = registry.timer(name(context.shortString(), "lookup.client.duration"));
        this.lookupService = registry.timer(name(context.shortString(), "lookup.service.duration"));
        this.outboudUnbindRequest = registry.meter(name(context.shortString(), "outbound.unbind.request"));
        this.outboundAppendEventsRequest = registry.meter(name(context.shortString(),
                                                               "outbound.append.events.request"));
        this.outboundAppendEventsResponse = registry.meter(name(context.shortString(),
                                                                "outbound.append.events.response"));
        this.outboundAppendKERLRequest = registry.meter(name(context.shortString(), "outbound.append.kerl.request"));
        this.outboundAppendKERLResponse = registry.meter(name(context.shortString(), "outbound.append.kerl.response"));
        this.outboundAppendWithAttachmentsRequest = registry.meter(name(context.shortString(),
                                                                        "outbound.append.with.attachments.request"));
        this.outboundAppendWithAttachmentsResponse = registry.meter(name(context.shortString(),
                                                                         "outbound.append.with.attachments.response"));
        this.outboundBindRequest = registry.meter(name(context.shortString(), "outbound.bind.request"));
        this.outboundGetAttachmentRequest = registry.meter(name(context.shortString(),
                                                                "outbound.get.attachments.request"));
        this.outboundGetAttachmentResponse = registry.meter(name(context.shortString(),
                                                                 "outbound.get.attachments.response"));
        this.outboundGetKERLRequest = registry.meter(name(context.shortString(), "outbound.bind.request"));
        this.outboundGetKERLResponse = registry.meter(name(context.shortString(), "outbound.get.kerl.response"));
        this.outboundGetKeyEventCoordsResponse = registry.meter(name(context.shortString(),
                                                                     "outbound.get.key.event.coords.request"));
        this.outboundGetKeyEventCoordsRequest = registry.meter(name(context.shortString(),
                                                                    "outbound.get.key.event.coords.response"));
        this.outboundGetKeyEventRequest = registry.meter(name(context.shortString(), "outbound.get.key.event.request"));
        this.outboundGetKeyEventResponse = registry.meter(name(context.shortString(),
                                                               "outbound.get.key.event.response"));
        this.outboundGetKeyStateCoordsRequest = registry.meter(name(context.shortString(),
                                                                    "outbound.get.key.state.coords.request"));
        this.outboundGetKeyStateCoordsResponse = registry.meter(name(context.shortString(),
                                                                     "outbound.get.key.state.coords.response"));
        this.outboundGetKeyStateRequest = registry.meter(name(context.shortString(), "outbound.get.key.state.request"));
        this.outboundGetKeyStateResponse = registry.meter(name(context.shortString(),
                                                               "outbound.get.key.state.request"));
        this.outboundLookupRequest = registry.meter(name(context.shortString(), "outbound.lookup.request"));
        this.outboundLookupResponse = registry.meter(name(context.shortString(), "outbound.lookup.response"));
        this.outboundPublishAttachmentsRequest = registry.meter(name(context.shortString(),
                                                                     "outbound.publish.attachments.request"));
        this.outboundPublishEventsRequest = registry.meter(name(context.shortString(),
                                                                "outbound.publish.events.request"));
        this.outboundPublishEventsResponse = registry.meter(name(context.shortString(),
                                                                 "outbound.publish.kerl.response"));
        this.outboundPublishKERLRequest = registry.meter(name(context.shortString(), "outbound.publish.kerl.request"));
        this.outboundPublishKERLResponse = registry.meter(name(context.shortString(),
                                                               "outbound.publish.kerl.response"));
        this.outboundValidatorRequest = registry.meter(name(context.shortString(), "outbound.lookup.request"));
        this.publishAttachmentsClient = registry.timer(name(context.shortString(),
                                                            "publish.attachments.client.duration"));
        this.publishAttachmentsService = registry.timer(name(context.shortString(),
                                                             "publish.attachments.service.duration"));
        this.publishEventsClient = registry.timer(name(context.shortString(), "publish.events.client.duration"));
        this.publishEventsService = registry.timer(name(context.shortString(), "publish.events.service.duration"));
        this.publishKERLClient = registry.timer(name(context.shortString(), "publish.kerl.client.duration"));
        this.publishKERLService = registry.timer(name(context.shortString(), "publish.kery.service.duration"));
        this.unbindClient = registry.timer(name(context.shortString(), "unbind.client.duration"));
        this.unbindService = registry.timer(name(context.shortString(), "unbind.service.duration"));
        this.validatorClient = registry.timer(name(context.shortString(), "validator.client.duration"));
        this.validatorService = registry.timer(name(context.shortString(), "validator.service.duration"));
    }

    @Override
    public Timer appendEventsClient() {
        return appendEventsClient;
    }

    @Override
    public Timer appendEventsService() {
        return appendEventsService;
    }

    @Override
    public Timer appendKERLClient() {
        return appendKERLClient;
    }

    @Override
    public Timer appendKERLService() {
        return appendKERLService;
    }

    @Override
    public Timer appendWithAttachmentsClient() {
        return appendWithAttachmentsClient;
    }

    @Override
    public Timer appendWithAttachmentsService() {
        return appendWithAttachmentsService;
    }

    @Override
    public Timer bindClient() {
        return bindClient;
    }

    @Override
    public Timer bindService() {
        return bindService;
    }

    @Override
    public Timer getAttachmentClient() {
        return getAttachmentClient;
    }

    @Override
    public Timer getAttachmentService() {
        return getAttachmentService;
    }

    @Override
    public Timer getKERLClient() {
        return getKERLClient;
    }

    @Override
    public Timer getKERLService() {
        return getKERLService;
    }

    @Override
    public Timer getKeyEventClient() {
        return getKeyEventClient;
    }

    @Override
    public Timer getKeyEventCoordsClient() {
        return getKeyEventCoordsClient;
    }

    @Override
    public Timer getKeyEventCoordsService() {
        return getKeyEventCoordsService;
    }

    @Override
    public Timer getKeyEventService() {
        return getKeyEventService;
    }

    @Override
    public Timer getKeyStateClient() {
        return getKeyStateClient;
    }

    @Override
    public Timer getKeyStateCoordsClient() {
        return getKeyStateCoordsClient;
    }

    @Override
    public Timer getKeyStateCoordsService() {
        return getKeyStateCoordsService;
    }

    @Override
    public Timer getKeyStateService() {
        return getKeyStateService;
    }

    @Override
    public Meter inboundAppendEventsRequest() {
        return inboundAppendEventsRequest;
    }

    @Override
    public Meter inboundAppendEventsResponse() {
        return inboundAppendEventsResponse;
    }

    @Override
    public Meter inboundAppendKERLRequest() {
        return inboundAppendKERLRequest;
    }

    @Override
    public Meter inboundAppendKERLResponse() {
        return inboundAppendKERLResponse;
    }

    @Override
    public Meter inboundAppendWithAttachmentsRequest() {
        return inboundAppendWithAttachmentsRequest;
    }

    @Override
    public Meter inboundAppendWithAttachmentsResponse() {
        return inboundAppendWithAttachmentsResponse;
    }

    @Override
    public Meter inboundBindRequest() {
        return inboundBindRequest;
    }

    @Override
    public Meter inboundGetAttachmentRequest() {
        return inboundGetAttachmentRequest;
    }

    @Override
    public Meter inboundGetAttachmentResponse() {
        return inboundGetAttachmentResponse;
    }

    @Override
    public Meter inboundGetKERLRequest() {
        return inboundGetKERLRequest;
    }

    @Override
    public Meter inboundGetKERLResponse() {
        return inboundGetKERLResponse;
    }

    @Override
    public Meter inboundGetKeyEventCoordsRequest() {
        return inboundGetKeyEventCoordsRequest;
    }

    @Override
    public Meter inboundGetKeyEventCoordsResponse() {
        return inboundGetKeyEventCoordsResponse;
    }

    @Override
    public Meter inboundGetKeyEventRequest() {
        return inboundGetKeyEventRequest;
    }

    @Override
    public Meter inboundGetKeyEventResponse() {
        return inboundGetKeyEventResponse;
    }

    @Override
    public Meter inboundGetKeyStateCoordsRequest() {
        return inboundGetKeyStateCoordsRequest;
    }

    @Override
    public Meter inboundGetKeyStateCoordsResponse() {
        return inboundGetKeyStateCoordsResponse;
    }

    @Override
    public Meter inboundGetKeyStateRequest() {
        return inboundGetKeyStateRequest;
    }

    @Override
    public Meter inboundGetKeyStateResponse() {
        return inboundGetKeyStateResponse;
    }

    @Override
    public Meter inboundLookupRequest() {
        return inboundLookupRequest;
    }

    @Override
    public Meter inboundLookupResponse() {
        return inboundLookupResponse;
    }

    @Override
    public Meter inboundPublishAttachmentsRequest() {
        return inboundPublishAttachmentsRequest;
    }

    @Override
    public Meter inboundPublishEventsRequest() {
        return inboundPublishEventsRequest;
    }

    @Override
    public Meter inboundPublishEventsResponse() {
        return inboundPublishEventsResponse;
    }

    @Override
    public Meter inboundPublishKERLRequest() {
        return inboundPublishKERLRequest;
    }

    @Override
    public Meter inboundPublishKERLResponse() {
        return inboundPublishKERLResponse;
    }

    @Override
    public Meter inboundUnbindRequest() {
        return inboundUnbindRequest;
    }

    @Override
    public Meter inboundValidatorRequest() {
        return inboundValidatorRequest;
    }

    @Override
    public Timer lookupClient() {
        return lookupClient;
    }

    @Override
    public Timer lookupService() {
        return lookupService;
    }

    @Override
    public Meter outboundAppendEventsRequest() {
        return outboundAppendEventsRequest;
    }

    @Override
    public Meter outboundAppendEventsResponse() {
        return outboundAppendEventsResponse;
    }

    @Override
    public Meter outboundAppendKERLRequest() {
        return outboundAppendKERLRequest;
    }

    @Override
    public Meter outboundAppendKERLResponse() {
        return outboundAppendKERLResponse;
    }

    @Override
    public Meter outboundAppendWithAttachmentsRequest() {
        return outboundAppendWithAttachmentsRequest;
    }

    @Override
    public Meter outboundAppendWithAttachmentsResponse() {
        return outboundAppendWithAttachmentsResponse;
    }

    @Override
    public Meter outboundBindRequest() {
        return outboundBindRequest;
    }

    @Override
    public Meter outboundGetAttachmentRequest() {
        return outboundGetAttachmentRequest;
    }

    @Override
    public Meter outboundGetAttachmentResponse() {
        return outboundGetAttachmentResponse;
    }

    @Override
    public Meter outboundGetKERLRequest() {
        return outboundGetKERLRequest;
    }

    @Override
    public Meter outboundGetKERLResponse() {
        return outboundGetKERLResponse;
    }

    @Override
    public Meter outboundGetKeyEventCoordsRequest() {
        return outboundGetKeyEventCoordsRequest;
    }

    @Override
    public Meter outboundGetKeyEventCoordsResponse() {
        return outboundGetKeyEventCoordsResponse;
    }

    @Override
    public Meter outboundGetKeyEventRequest() {
        return outboundGetKeyEventRequest;
    }

    @Override
    public Meter outboundGetKeyEventResponse() {
        return outboundGetKeyEventResponse;
    }

    @Override
    public Meter outboundGetKeyStateCoordsRequest() {
        return outboundGetKeyStateCoordsRequest;
    }

    @Override
    public Meter outboundGetKeyStateCoordsResponse() {
        return outboundGetKeyStateCoordsResponse;
    }

    @Override
    public Meter outboundGetKeyStateRequest() {
        return outboundGetKeyStateRequest;
    }

    @Override
    public Meter outboundGetKeyStateResponse() {
        return outboundGetKeyStateResponse;
    }

    @Override
    public Meter outboundLookupRequest() {
        return outboundLookupRequest;
    }

    @Override
    public Meter outboundLookupResponse() {
        return outboundLookupResponse;
    }

    @Override
    public Meter outboundPublishAttachmentsRequest() {
        return outboundPublishAttachmentsRequest;
    }

    @Override
    public Meter outboundPublishEventsRequest() {
        return outboundPublishEventsRequest;
    }

    @Override
    public Meter outboundPublishEventsResponse() {
        return outboundPublishEventsResponse;
    }

    @Override
    public Meter outboundPublishKERLRequest() {
        return outboundPublishKERLRequest;
    }

    @Override
    public Meter outboundPublishKERLResponse() {
        return outboundPublishKERLResponse;
    }

    @Override
    public Meter outboundUnbindRequest() {
        return outboudUnbindRequest;
    }

    @Override
    public Meter outboundValidatorRequest() {
        return outboundValidatorRequest;
    }

    @Override
    public Timer publishAttachmentsClient() {
        return publishAttachmentsClient;
    }

    @Override
    public Timer publishAttachmentsService() {
        return publishAttachmentsService;
    }

    @Override
    public Timer publishEventsClient() {
        return publishEventsClient;
    }

    @Override
    public Timer publishEventsService() {
        return publishEventsService;
    }

    @Override
    public Timer publishKERLClient() {
        return publishKERLClient;
    }

    @Override
    public Timer publishKERLService() {
        return publishKERLService;
    }

    @Override
    public Timer unbindClient() {
        return unbindClient;
    }

    @Override
    public Timer unbindService() {
        return unbindService;
    }

    @Override
    public Timer validatorClient() {
        return validatorClient;
    }

    @Override
    public Timer validatorService() {
        return validatorService;
    }
}
