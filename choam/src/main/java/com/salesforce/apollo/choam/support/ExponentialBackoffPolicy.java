package com.salesforce.apollo.choam.support;

import com.salesforce.apollo.utils.Entropy;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;

public class ExponentialBackoffPolicy {

    private final    Duration initialBackoff;
    private final    double   jitter;
    private final    Duration maxBackoff;
    private final    double   multiplier;
    private volatile Duration nextBackoff;

    public ExponentialBackoffPolicy(Duration initialBackoff, double jitter, Duration maxBackoff, double multiplier) {
        super();
        this.initialBackoff = initialBackoff;
        this.jitter = jitter;
        this.maxBackoff = maxBackoff;
        this.multiplier = multiplier;
        nextBackoff = initialBackoff;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Duration getInitialBackoff() {
        return initialBackoff;
    }

    public double getJitter() {
        return jitter;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public Duration nextBackoff() {
        long currentBackoffNanos = nextBackoff.toNanos();

        nextBackoff = Duration.ofNanos((long) Math.min(currentBackoffNanos * multiplier, maxBackoff.toNanos()));

        return Duration.ofNanos(
        currentBackoffNanos + uniformRandom(-jitter * currentBackoffNanos, jitter * currentBackoffNanos));
    }

    private long uniformRandom(double low, double high) {
        checkArgument(high >= low);
        double mag = high - low;
        return (long) (Entropy.nextBitsStreamDouble() * mag + low);
    }

    public static class Builder implements Cloneable {
        private Duration initialBackoff = Duration.ofMillis(10);
        private double   jitter         = .2;
        private Duration maxBackoff     = Duration.ofMillis(500);
        private double   multiplier     = 1.6;

        public ExponentialBackoffPolicy build() {
            return new ExponentialBackoffPolicy(initialBackoff, jitter, maxBackoff, multiplier);
        }

        public Builder clone() {
            try {
                return (Builder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException(e);
            }
        }

        public Duration getInitialBackoff() {
            return initialBackoff;
        }

        public Builder setInitialBackoff(Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
            return this;
        }

        public double getJitter() {
            return jitter;
        }

        public Builder setJitter(double jitter) {
            this.jitter = jitter;
            return this;
        }

        public Duration getMaxBackoff() {
            return maxBackoff;
        }

        public Builder setMaxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
            return this;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public Builder setMultiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }
    }
}
