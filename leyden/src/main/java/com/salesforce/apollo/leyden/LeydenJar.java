package com.salesforce.apollo.leyden;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.leyden.comm.binding.*;
import com.salesforce.apollo.leyden.comm.reconcile.*;
import com.salesforce.apollo.leyden.proto.*;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.RingCommunications;
import com.salesforce.apollo.utils.Entropy;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hal.hildebrand
 **/
public class LeydenJar {

    public static final  String                                                                       LEYDEN_JAR = "Leyden-Jar";
    private static final Logger                                                                       log        = LoggerFactory.getLogger(
    LeydenJar.class);
    private final        Context<Member>                                                              context;
    private final        RouterImpl.CommonCommunications<ReconciliationClient, ReconciliationService> reconComms;
    private final        RouterImpl.CommonCommunications<BinderClient, BinderService>                 binderComms;
    private final        double                                                                       fpr;
    private final        SigningMember                                                                member;
    private final        MVMap<byte[], Binding>                                                       bottled;
    private final        AtomicBoolean                                                                started    = new AtomicBoolean();
    private final        RingCommunications<Member, ReconciliationClient>                             reconcile;

    public LeydenJar(SigningMember member, Context<Member> context, Router communications, double fpr, MVStore store,
                     ReconciliationMetrics metrics, BinderMetrics binderMetrics) {
        this.context = context;
        this.member = member;
        var recon = new Reconciled();
        reconComms = communications.create(member, context.getId(), recon,
                                           ReconciliationService.class.getCanonicalName(),
                                           r -> new ReconciliationServer(r, communications.getClientIdentityProvider(),
                                                                         metrics), c -> Reckoning.getCreate(c, metrics),
                                           Reckoning.getLocalLoopback(recon, member));

        var borders = new Borders();
        binderComms = communications.create(member, context.getId(), borders, BinderService.class.getCanonicalName(),
                                            r -> new BinderServer(r, communications.getClientIdentityProvider(),
                                                                  binderMetrics), c -> Bind.getCreate(c, binderMetrics),
                                            Bind.getLocalLoopback(borders, member));
        this.fpr = fpr;
        bottled = store.openMap(LEYDEN_JAR,
                                new MVMap.Builder<byte[], Binding>().valueType(new ProtobufDatatype<Binding>(b -> {
                                    try {
                                        return Binding.parseFrom(b);
                                    } catch (InvalidProtocolBufferException e) {
                                        throw new RuntimeException(e);
                                    }
                                })));
        reconcile = new RingCommunications<>(this.context, member, reconComms);
    }

    public void start(Duration gossip) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        reconcile(Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()), gossip);
    }

    private Biff populate(long l, CombinedIntervals keyIntervals, double fpr) {
        return null;
    }

    private Update reconcile(ReconciliationClient link, Integer ring) {
        if (member.equals(link.getMember())) {
            return null;
        }
        CombinedIntervals keyIntervals = null;
        log.trace("Interval reconciliation on ring: {} with: {} on: {} intervals: {}", ring, link.getMember(),
                  member.getId(), keyIntervals);
        return link.reconcile(Intervals.newBuilder()
                                       .setRing(ring)
                                       .addAllIntervals(keyIntervals.toIntervals())
                                       .setHave(populate(Entropy.nextBitsStreamLong(), keyIntervals, fpr))
                                       .build());
    }

    private void reconcile(Optional<Update> result,
                           RingCommunications.Destination<Member, ReconciliationClient> destination,
                           ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        if (!result.isEmpty()) {
            try {
                Update update = result.get();
                log.trace("Received: {} events in interval reconciliation from: {} on: {}", update.getBindingsCount(),
                          destination.member().getId(), member.getId());
                update(update.getBindingsList());
            } catch (NoSuchElementException e) {
                log.debug("null interval reconciliation with {} : {} on: {}", destination.member().getId(),
                          member.getId(), e.getCause());
            }
        } else {
            log.trace("Received no events in interval reconciliation from: {} on: {}", destination.member().getId(),
                      member.getId());
        }
        if (started.get()) {
            scheduler.schedule(() -> reconcile(scheduler, duration), duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void reconcile(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        reconcile.execute((link, ring) -> reconcile(link, ring),
                          (futureSailor, destination) -> reconcile(futureSailor, destination, scheduler, duration));

    }

    private void update(List<Binding> bindings) {
    }

    private class Reconciled implements ReconciliationService {

        @Override
        public Update reconcile(Intervals request, Digest from) {
            return null;
        }

        @Override
        public void update(Updating request, Digest from) {

        }
    }

    private class Borders implements BinderService {

        @Override
        public void bind(Binding request, Digest from) {

        }

        @Override
        public void unbind(Key_ request, Digest from) {

        }
    }
}
