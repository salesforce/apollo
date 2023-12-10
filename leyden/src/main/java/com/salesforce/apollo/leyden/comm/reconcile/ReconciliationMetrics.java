package com.salesforce.apollo.leyden.comm.reconcile;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.EndpointMetrics;

public interface ReconciliationMetrics extends EndpointMetrics {
    Histogram inboundReconcile();

    Timer inboundReconcileTimer();

    Timer inboundUpdateTimer();

    Histogram reconcileReply();
}
