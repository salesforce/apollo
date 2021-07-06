/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.causal;

import static com.salesforce.apollo.utils.Utils.locked;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import com.salesfoce.apollo.utils.proto.Clock;
import com.salesfoce.apollo.utils.proto.IntStampedClock;
import com.salesforce.apollo.causal.BloomClock.ComparisonResult;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public record IntCausalClock(BloomClock clock, AtomicInteger sequenceNumber, Lock lock)
                            implements CausalClock<Integer, IntStampedClock> {

    @Override
    public Integer instant() {
        return sequenceNumber.get();
    }

    @Override
    public ComparisonResult compareTo(ClockValue b) {
        return locked(() -> clock.compareTo(b), lock);
    }

    @Override
    public BloomClockValue toBloomClockValue() {
        return locked(() -> clock.toBloomClockValue(), lock);
    }

    @Override
    public Clock toClock() {
        return locked(() -> clock.toClock(), lock);
    }

    @Override
    public IntStampedClock toStampedClock() {
        return locked(() -> IntStampedClock.newBuilder().setStamp(sequenceNumber.get()).setClock(clock.toClock())
                                           .build(),
                      lock);
    }

    @Override
    public Integer observe(Digest digest) {
        return locked(() -> {
            clock.add(digest);
            return instant();
        }, lock);
    }

    @Override
    public StampedClockValue<Integer, IntStampedClock> merge(StampedClockValue<Integer, IntStampedClock> b) {
        return locked(() -> {
            clock.merge(b);
            int sequence = Math.max(sequenceNumber.get(), b.instant());
            sequenceNumber.set(sequence);
            return new IntStampedClockValue(clock.toBloomClockValue(), sequence);
        }, lock);
    }

    @Override
    public IntStampedClock stamp(Digest digest) {
        return locked(() -> {
            clock.add(digest);
            int seq = sequenceNumber.incrementAndGet();
            return IntStampedClock.newBuilder().setStamp(seq).setClock(clock.toClock()).build();
        }, lock);
    }

    @Override
    public StampedClockValue<Integer, IntStampedClock> current() {
        return locked(() -> new IntStampedClockValue(clock.toBloomClockValue(), sequenceNumber.get()), lock);
    }

    @Override
    public Integer observeAll(Collection<Digest> digests) {
        return locked(() -> {
            clock.addAll(digests);
            return instant();
        }, lock);
    }

    public void reset() {
        locked(() -> {
            clock.reset();
            sequenceNumber.set(0);
        }, lock);
    }

    @Override
    public String toString() {
        return "ICC[" + current() + "]";
    }
}
