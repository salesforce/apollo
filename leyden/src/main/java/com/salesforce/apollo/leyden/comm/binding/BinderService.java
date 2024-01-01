package com.salesforce.apollo.leyden.comm.binding;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.leyden.proto.Binding;
import com.salesforce.apollo.leyden.proto.Bound;
import com.salesforce.apollo.leyden.proto.KeyAndToken;

/**
 * @author hal.hildebrand
 **/
public interface BinderService {
    void bind(Binding request, Digest from);

    Bound get(KeyAndToken request, Digest from);

    void unbind(KeyAndToken request, Digest from);
}
