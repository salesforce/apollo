package com.salesforce.apollo.ring;

import com.google.protobuf.Any;
import com.salesforce.apollo.archipelago.Link;

import java.io.Closeable;

/**
 * @author hal.hildebrand
 **/
public interface TestItService extends Link, Closeable {
    Any ping(Any request);
}
