/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.codahale.metrics.Timer;
import com.salesforce.apollo.archipelago.Enclave.RoutingClientIdentity;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.cryptography.proto.Digeste;
import com.salesforce.apollo.demesne.proto.DelegationUpdate;
import com.salesforce.apollo.demesne.proto.SignedDelegate;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.comms.Delegation;
import com.salesforce.apollo.model.comms.DelegationServer;
import com.salesforce.apollo.model.comms.DelegationService;
import com.salesforce.apollo.ring.RingCommunications;
import com.salesforce.apollo.utils.Entropy;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64;

/**
 * @author hal.hildebrand
 */
public class SubDomain extends Domain {
    private static final String DELEGATES_MAP_TEMPLATE = "delegates-%s";
    private final static Logger log                    = LoggerFactory.getLogger(SubDomain.class);

    private final MVMap<Digeste, SignedDelegate>         delegates;
    @SuppressWarnings("unused")
    private final Map<Digeste, Digest>                   delegations = new HashMap<>();
    private final double                                 fpr;
    private final Duration                               gossipInterval;
    private final int                                    maxTransfer;
    private final RingCommunications<Member, Delegation> ring;
    private final AtomicBoolean                          started     = new AtomicBoolean();
    private final MVStore                                store;
    private final ScheduledExecutorService               scheduler   = Executors.newScheduledThreadPool(1,
                                                                                                        Thread.ofVirtual()
                                                                                                              .factory());

    public SubDomain(ControlledIdentifierMember member, Builder params, Path checkpointBaseDir,
                     RuntimeParameters.Builder runtime, int maxTransfer, Duration gossipInterval, double fpr) {
        this(member, params, "jdbc:h2:mem:", checkpointBaseDir, runtime, maxTransfer, gossipInterval, fpr);
    }

    public SubDomain(ControlledIdentifierMember member, Builder params, RuntimeParameters.Builder runtime,
                     int maxTransfer, Duration gossipInterval, double fpr) {
        this(member, params, tempDirOf(member.getIdentifier()), runtime, maxTransfer, gossipInterval, fpr);
    }

    public SubDomain(ControlledIdentifierMember member, Builder prm, String dbURL, Path checkpointBaseDir,
                     RuntimeParameters.Builder runtime, int maxTransfer, Duration gossipInterval, double fpr) {
        super(member, prm, dbURL, checkpointBaseDir, runtime);
        this.maxTransfer = maxTransfer;
        this.fpr = fpr;
        final var identifier = qb64(member.getId());
        final var builder = params.mvBuilder().clone();
        builder.setFileName(checkpointBaseDir.resolve(identifier).toFile());
        store = builder.build();
        delegates = store.openMap(DELEGATES_MAP_TEMPLATE.formatted(identifier));
        CommonCommunications<Delegation, ?> comms = params.communications()
                                                          .create(member, params.context().getId(), delegation(),
                                                                  "delegates", r -> new DelegationServer(
                                                          (RoutingClientIdentity) params.communications()
                                                                                        .getClientIdentityProvider(), r,
                                                          null));
        ring = new RingCommunications<Member, Delegation>(params.context(), member, comms);
        this.gossipInterval = gossipInterval;

    }

    public SubDomain(ControlledIdentifierMember member, Builder params, String dbURL, RuntimeParameters.Builder runtime,
                     int maxTransfer, Duration gossipInterval, double fpr) {
        this(member, params, dbURL, tempDirOf(member.getIdentifier()), runtime, maxTransfer, gossipInterval, fpr);
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        super.start();
        Duration initialDelay = gossipInterval.plusMillis(Entropy.nextBitsStreamLong(gossipInterval.toMillis()));
        log.trace("Starting SubDomain[{}:{}]", params.context().getId(), member.getId());
        scheduler.schedule(() -> oneRound(), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        try {
            super.stop();
        } finally {
            store.close(500);
        }
    }

    private DelegationService delegation() {
        return new DelegationService() {
            @Override
            public DelegationUpdate gossip(Biff identifiers, Digest from) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void update(DelegationUpdate update, Digest from) {
                // TODO Auto-generated method stub

            }
        };
    }

    private DelegationUpdate gossipRound(Delegation link, Integer ring) {
        return link.gossip(have());
    }

    private void handle(Optional<DelegationUpdate> result,
                        RingCommunications.Destination<Member, Delegation> destination, Timer.Context timer) {
        if (!started.get() || destination.link() == null) {
            if (timer != null) {
                timer.stop();
            }
            return;
        }
        try {
            if (result.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                log.trace("no update from {} on: {}", destination.member().getId(), member.getId());
                return;
            }
            DelegationUpdate update = result.get();
            if (update.equals(DelegationUpdate.getDefaultInstance())) {
                return;
            }
            log.trace("gossip update with {} on: {}", destination.member().getId(), member.getId());
            destination.link()
                       .update(update(update, DelegationUpdate.newBuilder()
                                                              .setRing(destination.ring())
                                                              .setHave(have())).build());
        } finally {
            if (timer != null) {
                timer.stop();
            }
            if (started.get()) {
                scheduler.schedule(() -> oneRound(), gossipInterval.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private Biff have() {
        DigestBloomFilter bff = new DigestBloomFilter(Entropy.nextBitsStreamLong(), delegates.size(), fpr);
        delegates.keySet().stream().map(d -> Digest.from(d)).forEach(d -> bff.add(d));
        return bff.toBff();
    }

    private void oneRound() {
        Thread.ofVirtual().start(() -> {
            Timer.Context timer = null;
            try {
                ring.execute((link, ring) -> gossipRound(link, ring),
                             (result, destination) -> handle(result, destination, timer));
            } catch (Throwable e) {
                log.error("Error in delegation gossip in SubDomain[{}:{}]", params.context().getId(), member.getId(),
                          e);
            }
        });
    }

    private DelegationUpdate.Builder update(DelegationUpdate update, DelegationUpdate.Builder builder) {
        update.getUpdateList().forEach(sd -> delegates.putIfAbsent(sd.getDelegate().getDelegate(), sd));
        BloomFilter<Digest> bff = BloomFilter.from(update.getHave());
        delegates.entrySet()
                 .stream()
                 .filter(e -> !bff.contains(Digest.from(e.getKey())))
                 .limit(maxTransfer)
                 .forEach(e -> builder.addUpdate(e.getValue()));
        return builder;
    }
}
