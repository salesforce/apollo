/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.time.Clock;
import java.util.concurrent.Callable;

import org.h2.value.ValueTimestampTimeZone;

import com.salesforce.apollo.state.SystemClock;

public final class CurrentTimestamp {
    
    private static final ThreadLocal<Clock> CLOCK = ThreadLocal.withInitial(() -> SystemClock.systemUTC());
    
    public static <T> T with(Clock clock, Callable<T> closure) throws Exception {
        Clock prev = CLOCK.get();
        try {
            CLOCK.set(clock);
            return  closure.call();
        } finally {
            CLOCK.set(prev);
        }
    }
    
    public static void with(Clock clock, Runnable closure) {
        Clock prev = CLOCK.get();
        try {
            CLOCK.set(clock);
            closure.run();
        } finally {
            CLOCK.set(prev);
        }
    }

    /*
     * Signatures of methods should match with
     * h2/src/java9/src/org/h2/util/CurrentTimestamp.java and precompiled
     * h2/src/java9/precompiled/org/h2/util/CurrentTimestamp.class.
     */

    /**
     * Returns current timestamp.
     *
     * @return current timestamp
     */
    public static ValueTimestampTimeZone get() {
        return DateTimeUtils.timestampTimeZoneFromMillis(CLOCK.get().millis());
    }

    private CurrentTimestamp() {
    }

}
