package com.salesforce.apollo.leyden.comm.reconcile;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.leyden.proto.Intervals;
import com.salesforce.apollo.leyden.proto.Update;
import com.salesforce.apollo.leyden.proto.Updating;

/**
 * @author hal.hildebrand
 **/
public interface ReconciliationClient extends Link {
    Update reconcile(Intervals intervals);

    void update(Updating updating);
}
