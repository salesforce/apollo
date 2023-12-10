package com.salesforce.apollo.leyden.comm;

import com.salesforce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.thoth.proto.Update;
import com.salesforce.apollo.thoth.proto.Updating;
import com.salesforce.apollo.cryptography.Digest;

/**
 * @author hal.hildebrand
 **/
public interface ReconciliationService {
    Update reconcile(Intervals request, Digest from);

    void update(Updating request, Digest from);
}
