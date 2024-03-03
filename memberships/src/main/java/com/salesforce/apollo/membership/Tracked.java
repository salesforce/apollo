package com.salesforce.apollo.membership;

import com.salesforce.apollo.cryptography.Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 **/
public class Tracked<M extends Member> {
    private static final Logger log = LoggerFactory.getLogger(Tracked.class);

    final AtomicBoolean active = new AtomicBoolean(false);
    final M             member;
    Digest[] hashes;

    public Tracked(M member, Supplier<Digest[]> hashes) {
        this.member = member;
        this.hashes = hashes.get();
    }

    public boolean activate() {
        var activated = active.compareAndSet(false, true);
        if (activated) {
            log.trace("Activated: {}", member.getId());
        }
        return activated;
    }

    public Digest hash(int index) {
        return hashes[index];
    }

    public boolean isActive() {
        return active.get();
    }

    public M member() {
        return member;
    }

    public boolean offline() {
        var offlined = active.compareAndSet(true, false);
        if (offlined) {
            log.trace("Offlined: {}", member.getId());
        }
        return offlined;
    }

    @Override
    public String toString() {
        return String.format("%s:%s %s", member, active.get(), Arrays.asList(hashes));
    }

    void rebalance(int ringCount, Context<M> context) {
        final var newHashes = new Digest[ringCount];
        System.arraycopy(hashes, 0, newHashes, 0, Math.min(ringCount, hashes.length));
        for (int i = Math.min(ringCount, hashes.length); i < newHashes.length; i++) {
            newHashes[i] = context.hashFor(member.getId(), i);
        }
        hashes = newHashes;
    }
}
