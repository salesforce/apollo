package com.salesforce.apollo.leyden.comm.binding;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.leyden.proto.Bound;
import com.salesforce.apollo.leyden.proto.Key_;

/**
 * @author hal.hildebrand
 **/
public interface BinderClient extends Link {

    void bind(Bound binding);

    void unbind(Key_ key);
}
