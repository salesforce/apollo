package com.salesforce.apollo.leyden.comm.reconcile;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.leyden.proto.Intervals;
import com.salesforce.apollo.leyden.proto.Update;
import com.salesforce.apollo.leyden.proto.Updating;

/**
 * @author hal.hildebrand
 **/
public interface ReconciliationService {
    Update reconcile(Intervals request, Digest from);

    void update(Updating request, Digest from);
}
