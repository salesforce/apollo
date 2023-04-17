/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.demesne.proto.DelegationUpdate;
import com.salesfoce.apollo.demesne.proto.SignedDelegate;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.archipelago.Router.CommonCommunications;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.comms.Delegation;
import com.salesforce.apollo.model.comms.DelegationServer;
import com.salesforce.apollo.model.comms.DelegationService;
import com.salesforce.apollo.ring.RingCommunications;
import com.salesforce.apollo.ring.RingCommunications.Destination;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * @author hal.hildebrand
 *
 */
public class SubDomain extends Domain {
    private static final String DELEGATES_MAP_TEMPLATE  = "delegates-%s";
    private static final String DELEGATORS_MAP_TEMPLATE = "delegators-%s";
    private final static Logger log                     = LoggerFactory.getLogger(SubDomain.class);

    private final MVMap<Digeste, SignedDelegate>         delegates;
    @SuppressWarnings("unused")
    private final MVMap<Digeste, Digest>                 delegators;
    private final double                                 fpr;
    private final Duration                               gossipInterval;
    private final int                                    maxTransfer;
    private final RingCommunications<Member, Delegation> ring;
    private ScheduledFuture<?>                           scheduled;
    private final AtomicBoolean                          started = new AtomicBoolean();
    private final MVStore                                store;

    public SubDomain(ControlledIdentifierMember member, Builder params, Path checkpointBaseDir,
                     RuntimeParameters.Builder runtime, TransactionConfiguration txnConfig, int maxTransfer,
                     Duration gossipInterval, double fpr) {
        this(member, params, "jdbc:h2:mem:", checkpointBaseDir, runtime, txnConfig, maxTransfer, gossipInterval, fpr);
    }

    public SubDomain(ControlledIdentifierMember member, Builder params, RuntimeParameters.Builder runtime,
                     TransactionConfiguration txnConfig, int maxTransfer, Duration gossipInterval, double fpr) {
        this(member, params, tempDirOf(member.getIdentifier()), runtime, txnConfig, maxTransfer, gossipInterval, fpr);
    }

    public SubDomain(ControlledIdentifierMember member, Builder prm, String dbURL, Path checkpointBaseDir,
                     RuntimeParameters.Builder runtime, TransactionConfiguration txnConfig, int maxTransfer,
                     Duration gossipInterval, double fpr) {
        super(member, prm, dbURL, checkpointBaseDir, runtime, txnConfig);
        this.maxTransfer = maxTransfer;
        this.fpr = fpr;
        final var identifier = qb64(member.getId());
        final var builder = params.mvBuilder().clone();
        builder.setFileName(checkpointBaseDir.resolve(identifier).toFile());
        store = builder.build();
        delegates = store.openMap(DELEGATES_MAP_TEMPLATE.formatted(identifier));
        delegators = store.openMap(DELEGATORS_MAP_TEMPLATE.formatted(identifier));
        CommonCommunications<Delegation, ?> comms = params.communications()
                                                          .create(member, params.context().getId(), delegation(),
                                                                  "delegation",
                                                                  r -> new DelegationServer(params.communications()
                                                                                                  .getClientIdentityProvider(),
                                                                                            r, null));
        ring = new RingCommunications<Member, Delegation>(params.context(), member, comms, params.exec());
        this.gossipInterval = gossipInterval;

    }

    public SubDomain(ControlledIdentifierMember member, Builder params, String dbURL, RuntimeParameters.Builder runtime,
                     TransactionConfiguration txnConfig, int maxTransfer, Duration gossipInterval, double fpr) {
        this(member, params, dbURL, tempDirOf(member.getIdentifier()), runtime, txnConfig, maxTransfer, gossipInterval,
             fpr);
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        super.start();
        Duration initialDelay = gossipInterval.plusMillis(Entropy.nextBitsStreamLong(gossipInterval.toMillis()));
        log.trace("Starting SubDomain[{}:{}]", params.context().getId(), member.getId());
        params.runtime().scheduler().schedule(() -> oneRound(), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (scheduled != null) {
            scheduled.cancel(true);
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

    private ListenableFuture<DelegationUpdate> gossipRound(Delegation link, Integer ring) {
        return link.gossip(have());
    }

    private void handle(Optional<ListenableFuture<DelegationUpdate>> futureSailor,
                        Destination<Member, Delegation> destination, Timer.Context timer) {
        if (!started.get() || destination.link() == null) {
            if (timer != null) {
                timer.stop();
            }
            return;
        }
        try {
            if (futureSailor.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                log.trace("no update from {} on: {}", destination.member().getId(), member.getId());
                return;
            }
            DelegationUpdate update;
            try {
                update = futureSailor.get().get();
            } catch (InterruptedException e) {
                log.error("error gossiping with {} on: {}", destination.member().getId(), member.getId(), e);
                return;
            } catch (ExecutionException e) {
                var cause = e.getCause();
                if (cause instanceof StatusRuntimeException sre) {
                    final var code = sre.getStatus().getCode();
                    if (code.equals(Status.UNAVAILABLE.getCode()) || code.equals(Status.NOT_FOUND.getCode()) ||
                        code.equals(Status.UNIMPLEMENTED.getCode()) ||
                        code.equals(Status.RESOURCE_EXHAUSTED.getCode())) {
                        return;
                    }
                }
                log.warn("error gossiping with {} on: {}", destination.member().getId(), member.getId(), cause);
                return;
            }
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
                scheduled = params.runtime()
                                  .scheduler()
                                  .schedule(() -> oneRound(), gossipInterval.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private Biff have() {
        DigestBloomFilter bff = new DigestBloomFilter(Entropy.nextBitsStreamLong(), delegates.size(), fpr);
        delegates.keySet().stream().map(d -> Digest.from(d)).forEach(d -> bff.add(d));
        return bff.toBff();
    }

    private void oneRound() {
        Timer.Context timer = null;
        try {
            ring.execute((link, ring) -> gossipRound(link, ring),
                         (futureSailor, destination) -> handle(futureSailor, destination, timer));
        } catch (Throwable e) {
            log.error("Error in delegation gossip in SubDomain[{}:{}]", params.context().getId(), member.getId(), e);
        }
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
