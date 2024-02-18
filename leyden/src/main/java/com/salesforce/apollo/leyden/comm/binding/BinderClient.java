package com.salesforce.apollo.leyden.comm.binding;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.leyden.proto.Binding;
import com.salesforce.apollo.leyden.proto.Bound;
import com.salesforce.apollo.leyden.proto.Key;

/**
 * @author hal.hildebrand
 **/
public interface BinderClient extends Link {

    void bind(Binding binding);

    Bound get(Key key);

    void unbind(Key key);
}
