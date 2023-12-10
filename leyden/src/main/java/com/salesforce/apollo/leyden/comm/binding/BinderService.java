package com.salesforce.apollo.leyden.comm.binding;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.leyden.proto.Binding;
import com.salesforce.apollo.leyden.proto.Key_;

/**
 * @author hal.hildebrand
 **/
public interface BinderService {
    void bind(Binding request, Digest from);

    void unbind(Key_ request, Digest from);
}
