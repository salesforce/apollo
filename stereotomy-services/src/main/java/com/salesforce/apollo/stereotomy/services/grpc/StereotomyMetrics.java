/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.BandwidthMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface StereotomyMetrics extends BandwidthMetrics {

    Timer appendEventsClient();

    Timer appendEventsService();

    Timer appendKERLClient();

    Timer appendKERLService();

    Timer appendWithAttachmentsClient();

    Timer appendWithAttachmentsService();

    Timer bindClient();

    Timer bindService();

    Timer getAttachmentClient();

    Timer getAttachmentService();

    Timer getKERLClient();

    Timer getKERLService();

    Timer getKeyEventClient();

    Timer getKeyEventCoordsClient();

    Timer getKeyEventCoordsService();

    Timer getKeyEventService();

    Timer getKeyStateClient();

    Timer getKeyStateCoordsClient();

    Timer getKeyStateCoordsService();

    Timer getKeyStateService();

    Meter inboundAppendEventsRequest();

    Meter inboundAppendEventsResponse();

    Meter inboundAppendKERLRequest();

    Meter inboundAppendKERLResponse();

    Meter inboundAppendWithAttachmentsRequest();

    Meter inboundAppendWithAttachmentsResponse();

    Meter inboundBindRequest();

    Meter inboundGetAttachmentRequest();

    Meter inboundGetAttachmentResponse();

    Meter inboundGetKERLRequest();

    Meter inboundGetKERLResponse();

    Meter inboundGetKeyEventCoordsRequest();

    Meter inboundGetKeyEventCoordsResponse();

    Meter inboundGetKeyEventRequest();

    Meter inboundGetKeyEventResponse();

    Meter inboundGetKeyStateCoordsRequest();

    Meter inboundGetKeyStateCoordsResponse();

    Meter inboundGetKeyStateRequest();

    Meter inboundGetKeyStateResponse();

    Meter inboundLookupRequest();

    Meter inboundLookupResponse();

    Meter inboundPublishAttachmentsRequest();

    Meter inboundPublishEventsRequest();

    Meter inboundPublishEventsResponse();

    Meter inboundPublishKERLRequest();

    Meter inboundPublishKERLResponse();

    Meter inboundUnbindRequest();

    Meter inboundValidatorRequest();

    Timer lookupClient();

    Timer lookupService();

    Meter outboundAppendEventsRequest();

    Meter outboundAppendEventsResponse();

    Meter outboundAppendKERLRequest();

    Meter outboundAppendKERLResponse();

    Meter outboundAppendWithAttachmentsRequest();

    Meter outboundAppendWithAttachmentsResponse();

    Meter outboundBindRequest();

    Meter outboundGetAttachmentRequest();

    Meter outboundGetAttachmentResponse();

    Meter outboundGetKERLRequest();

    Meter outboundGetKERLResponse();

    Meter outboundGetKeyEventCoordsRequest();

    Meter outboundGetKeyEventCoordsResponse();

    Meter outboundGetKeyEventRequest();

    Meter outboundGetKeyEventResponse();

    Meter outboundGetKeyStateCoordsRequest();

    Meter outboundGetKeyStateCoordsResponse();

    Meter outboundGetKeyStateRequest();

    Meter outboundGetKeyStateResponse();

    Meter outboundLookupRequest();

    Meter outboundLookupResponse();

    Meter outboundPublishAttachmentsRequest();

    Meter outboundPublishEventsRequest();

    Meter outboundPublishEventsResponse();

    Meter outboundPublishKERLRequest();

    Meter outboundPublishKERLResponse();

    Meter outboundUnbindRequest();

    Meter outboundValidatorRequest();

    Timer publishAttachmentsClient();

    Timer publishAttachmentsService();

    Timer publishEventsClient();

    Timer publishEventsService();

    Timer publishKERLClient();

    Timer publishKERLService();

    Timer unbindClient();

    Timer unbindService();

    Timer validatorClient();

    Timer validatorService();

}
