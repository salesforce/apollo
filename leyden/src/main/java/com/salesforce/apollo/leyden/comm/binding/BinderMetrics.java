package com.salesforce.apollo.leyden.comm.binding;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.EndpointMetrics;

/**
 * @author hal.hildebrand
 **/
public interface BinderMetrics extends EndpointMetrics {
    Histogram inboundBind();

    Timer inboundBindTimer();

    Histogram inboundUnbind();

    Timer inboundUnbindTimer();
}
