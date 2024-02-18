package com.salesforce.apollo.leyden;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.macasaet.fernet.Token;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.leyden.comm.binding.*;
import com.salesforce.apollo.leyden.comm.reconcile.*;
import com.salesforce.apollo.leyden.proto.*;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.RingCommunications;
import com.salesforce.apollo.ring.RingIterator;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Hex;
import com.salesforce.apollo.utils.Utils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author hal.hildebrand
 **/
public class LeydenJar {
    public static final  String LEYDEN_JAR = "Leyden-Jar";
    private static final Logger log        = LoggerFactory.getLogger(LeydenJar.class);
    private static final String DIGESTS    = "Digests";

    private final Context<Member>                                                              context;
    private final RouterImpl.CommonCommunications<ReconciliationClient, ReconciliationService> reconComms;
    private final RouterImpl.CommonCommunications<BinderClient, BinderService>                 binderComms;
    private final DigestAlgorithm                                                              algorithm;
    private final double                                                                       fpr;
    private final SigningMember                                                                member;
    private final MVMap<Digest, Bound>                                                         bottled;
    private final MVMap<Digest, Digest>                                                        digests;
    private final AtomicBoolean                                                                started   = new AtomicBoolean();
    private final RingCommunications<Member, ReconciliationClient>                             reconcile;
    private final NavigableMap<Digest, List<ConsensusState>>                                   pending   = new ConcurrentSkipListMap<>();
    private final Borders                                                                      borders;
    private final Reconciled                                                                   recon;
    private final TemporalAmount                                                               operationTimeout;
    private final Duration                                                                     operationsFrequency;
    private final ScheduledExecutorService                                                     scheduler = Executors.newScheduledThreadPool(
    1, Thread.ofVirtual().factory());
    private final OpValidator                                                                  validator;

    public LeydenJar(OpValidator validator, TemporalAmount operationTimeout, SigningMember member,
                     Context<Member> context, Duration operationsFrequency, Router communications, double fpr,
                     DigestAlgorithm algorithm, MVStore store, ReconciliationMetrics metrics,
                     BinderMetrics binderMetrics) {
        this.validator = validator;
        this.context = context;
        this.member = member;
        this.algorithm = algorithm;
        recon = new Reconciled();
        this.operationTimeout = operationTimeout;
        this.operationsFrequency = operationsFrequency;
        reconComms = communications.create(member, context.getId(), recon,
                                           ReconciliationService.class.getCanonicalName(),
                                           r -> new ReconciliationServer(r, communications.getClientIdentityProvider(),
                                                                         metrics), c -> Reckoning.getCreate(c, metrics),
                                           Reckoning.getLocalLoopback(recon, member));

        borders = new Borders();
        binderComms = communications.create(member, context.getId(), borders, BinderService.class.getCanonicalName(),
                                            r -> new BinderServer(r, communications.getClientIdentityProvider(),
                                                                  binderMetrics), c -> Bind.getCreate(c, binderMetrics),
                                            Bind.getLocalLoopback(borders, member));
        this.fpr = fpr;
        bottled = store.openMap(LEYDEN_JAR, new MVMap.Builder<Digest, Bound>().keyType(new DigestDatatype(algorithm))
                                                                              .valueType(new BoundDatatype()));
        digests = store.openMap(DIGESTS, new MVMap.Builder<Digest, Digest>().keyType(new DigestDatatype(algorithm))
                                                                            .valueType(new DigestDatatype(algorithm)));
        reconcile = new RingCommunications<>(this.context, member, reconComms);
    }

    public void bind(Binding bound) {
        var key = bound.getBound().getKey();
        log.info("Bind: {} on: {}", Hex.hex(key.toByteArray()), member.getId());
        var hash = algorithm.digest(key);
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<String>();
        var gathered = HashMultiset.<String>create();
        var iterate = new RingIterator<Member, BinderClient>(operationsFrequency, context, member, scheduler,
                                                             binderComms);
        iterate.iterate(hash, null, (link, r) -> {
                            link.bind(bound);
                            return "";
                        }, () -> failedMajority(result, maxCount(gathered)),
                        (tally, futureSailor, destination) -> write(result, gathered, tally, futureSailor, hash,
                                                                    isTimedOut, destination),
                        t -> failedMajority(result, maxCount(gathered)));
        try {
            result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    public Bound get(Key keyAndToken) {
        var hash = algorithm.digest(keyAndToken.getKey());
        log.info("Get: {} on: {}", hash, member.getId());
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Bound>();
        var gathered = HashMultiset.<Bound>create();
        var iterate = new RingIterator<Member, BinderClient>(operationsFrequency, context, member, scheduler,
                                                             binderComms);
        iterate.iterate(hash, null, (link, r) -> {
                            var bound = link.get(keyAndToken);
                            log.debug("Get {}: bound: <{}:{}> from: {} on: {}", hash, bound.getKey().toStringUtf8(),
                                      bound.getValue().toStringUtf8(), link.getMember().getId(), member.getId());
                            return bound;
                        }, () -> failedMajority(result, maxCount(gathered)),
                        (tally, futureSailor, destination) -> read(result, gathered, tally, futureSailor, hash,
                                                                   isTimedOut, destination, "Get"),
                        t -> failedMajority(result, maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    public void start(Duration gossip) {
        start(gossip, null);
    }

    public void start(Duration gossip, Predicate<Token> validator) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        log.info("Starting context: {}:{} on: {}", context.getId(), System.identityHashCode(context), member.getId());
        binderComms.register(context.getId(), borders, validator);
        reconComms.register(context.getId(), recon, validator);
        reconcile(scheduler, gossip);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping: {}", member.getId());
        binderComms.deregister(context.getId());
        reconComms.deregister(context.getId());
    }

    public void unbind(Key keyAndToken) {
        var key = keyAndToken.toByteArray();
        var hash = algorithm.digest(key);
        log.info("Unbind: {} on: {}", hash, member.getId());
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<String>();
        var gathered = HashMultiset.<String>create();
        var iterate = new RingIterator<Member, BinderClient>(operationsFrequency, context, member, scheduler,
                                                             binderComms);
        iterate.iterate(hash, null, (link, r) -> {
                            link.unbind(keyAndToken);
                            return "";
                        }, () -> failedMajority(result, maxCount(gathered)),
                        (tally, futureSailor, destination) -> read(result, gathered, tally, futureSailor, hash,
                                                                   isTimedOut, destination, "Unbind"),
                        t -> failedMajority(result, maxCount(gathered)));
        try {
            result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    private void add(Digest hash, Bound bound, Digest digest) {
        var existing = digests.get(hash);
        if (existing == null || !existing.equals(digest)) {
            bottled.put(hash, bound);
            digests.put(hash, digest);
            log.info("Add: <{}> on: {}", bound.getKey().toStringUtf8(), member.getId());
        }
    }

    private Stream<Digest> bindingsIn(KeyInterval i) {
        Iterator<Digest> it = new Iterator<Digest>() {
            private final Iterator<Digest> iterate = bottled.keyIterator(i.getBegin());
            private Digest next;

            {
                if (iterate.hasNext()) {
                    next = iterate.next();
                    if (next.compareTo(i.getEnd()) > 0) {
                        next = null; // got nothing
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Digest next() {
                var returned = next;
                next = null;
                if (returned == null) {
                    throw new NoSuchElementException();
                }
                if (iterate.hasNext()) {
                    next = iterate.next();
                    if (next.compareTo(i.getEnd()) > 0) {
                        next = null; // got nothing
                    }
                }
                return returned;
            }
        };
        Iterable<Digest> iterable = () -> it;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private void failedMajority(CompletableFuture<?> result, int maxAgree) {
        result.completeExceptionally(new NoSuchElementException(
        "Unable to achieve majority read, max: %s required: %s on: %s".formatted(maxAgree, context.majority(),
                                                                                 member.getId())));
    }

    private boolean invalid(Digest from, int ring) {
        if (ring >= context.getRingCount() || ring < 0) {
            log.warn("invalid ring: {} from: {} on: {}", ring, from, member.getId());
            return true;
        }

        Member predecessor = context.ring(ring).predecessor(member);
        if (predecessor == null || !from.equals(predecessor.getId())) {
            log.warn("Invalid, not predecessor: {}, ring: {} expected: {} on: {}", from, ring, predecessor.getId(),
                     member.getId());
            return true;
        }
        return false;
    }

    private CombinedIntervals keyIntervals() {
        List<KeyInterval> intervals = new ArrayList<>();
        for (int i = 0; i < context.getRingCount(); i++) {
            Ring<Member> ring = context.ring(i);
            Member predecessor = ring.predecessor(member);
            if (predecessor == null) {
                continue;
            }

            Digest begin = ring.hash(predecessor);
            Digest end = ring.hash(member);

            if (begin.compareTo(end) > 0) { // wrap around the origin of the ring
                intervals.add(new KeyInterval(end, algorithm.getLast()));
                intervals.add(new KeyInterval(algorithm.getOrigin(), begin));
            } else {
                intervals.add(new KeyInterval(begin, end));
            }
        }
        return new CombinedIntervals(intervals);
    }

    private <T> Multiset.Entry<T> max(HashMultiset<T> gathered) {
        return gathered.entrySet().stream().max(Ordering.natural().onResultOf(Multiset.Entry::getCount)).orElse(null);
    }

    private int maxCount(HashMultiset<?> gathered) {
        final var max = gathered.entrySet().stream().max(Ordering.natural().onResultOf(Multiset.Entry::getCount));
        return max.isEmpty() ? 0 : max.get().getCount();
    }

    private Biff populate(long seed, CombinedIntervals keyIntervals) {
        BloomFilter.DigestBloomFilter bff = new BloomFilter.DigestBloomFilter(seed, Math.max(bottled.size(), 100), fpr);
        bottled.keyIterator(algorithm.getOrigin()).forEachRemaining(d -> {
            if (keyIntervals.test(d)) {
                var bound = bottled.get(d);
                if (bound != null) {
                    var digest = algorithm.digest(bound.toByteString());
                    bff.add(digest);
                }
            }
        });
        return bff.toBff();
    }

    private <B> boolean read(CompletableFuture<B> result, HashMultiset<B> gathered, AtomicInteger tally,
                             Optional<B> futureSailor, Digest hash, Supplier<Boolean> isTimedOut,
                             RingCommunications.Destination<Member, BinderClient> destination, String op) {
        if (futureSailor.isEmpty()) {
            log.debug("{}: {} empty from: {}  on: {}", op, hash, destination.member().getId(), member.getId());
            return !isTimedOut.get();
        }
        var content = futureSailor.get();
        if (content != null) {
            log.debug("{}: {} from: {}  on: {}", op, hash, destination.member().getId(), member.getId());
            gathered.add(content);
            var max = max(gathered);
            if (max != null) {
                tally.set(max.getCount());
                if (max.getCount() > context.toleranceLevel()) {
                    result.complete(max.getElement());
                    log.debug("Majority {}: {} achieved: {} on: {}", op, max.getCount(), hash, member.getId());
                    return false;
                }
            }
            return !isTimedOut.get();
        } else {
            log.debug("Failed {}: {} from: {}  on: {}", op, hash, destination.member().getId(), member.getId());
            return !isTimedOut.get();
        }
    }

    private Update reconcile(ReconciliationClient link, Integer ring) {
        if (member.equals(link.getMember())) {
            log.debug("Reconciliation on ring: {} with self on: {} ", ring, member.getId());
            return null;
        }
        CombinedIntervals keyIntervals = keyIntervals();
        log.debug("Interval reconciliation on ring: {} with: {} intervals: {} on: {} ", ring, link.getMember().getId(),
                  keyIntervals, member.getId());
        return link.reconcile(Intervals.newBuilder()
                                       .setRing(ring)
                                       .addAllIntervals(keyIntervals.toIntervals())
                                       .setHave(populate(Entropy.nextBitsStreamLong(), keyIntervals))
                                       .build());
    }

    private void reconcile(Optional<Update> result,
                           RingCommunications.Destination<Member, ReconciliationClient> destination,
                           ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        if (result.isPresent()) {
            try {
                Update update = result.get();
                log.trace("Received: {} events in interval reconciliation from: {} on: {}", update.getBindingsCount(),
                          destination.member().getId(), member.getId());
                update(update.getBindingsList(), destination.member().getId());
            } catch (NoSuchElementException e) {
                log.debug("null interval reconciliation with {} on: {}", destination.member().getId(), member.getId(),
                          e.getCause());
            }
        } else {
            log.trace("Received no events in interval reconciliation from: {} on: {}", destination.member().getId(),
                      member.getId());
        }
        if (started.get()) {
            scheduler.schedule(() -> Thread.ofVirtual().start(Utils.wrapped(() -> reconcile(scheduler, duration), log)),
                               duration.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private void reconcile(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        Thread.ofVirtual()
              .start(() -> reconcile.execute(this::reconcile,
                                             (futureSailor, destination) -> reconcile(futureSailor, destination,
                                                                                      scheduler, duration)));
    }

    /**
     * Reconcile the intervals for our partner
     *
     * @param intervals - the relevant intervals of keys and the  digests of these keys the partner already have
     * @return the Update.Builder of missing keys, based on the supplied intervals
     */
    private Update.Builder reconcile(Intervals intervals) {
        var biff = BloomFilter.from(intervals.getHave());
        var update = Update.newBuilder();
        intervals.getIntervalsList()
                 .stream()
                 .map(KeyInterval::new)
                 .flatMap(this::bindingsIn)
                 .peek(d -> log.debug("reconcile digest: {} on: {}", d, member.getId()))
                 .filter(d -> !biff.contains(d))
                 .peek(d -> log.debug("filtered reconcile digest: {} on: {}", d, member.getId()))
                 .map(d1 -> bottled.get(d1))
                 .filter(Objects::nonNull)
                 .forEach(update::addBindings);
        return update;
    }

    private void update(List<Bound> bindings, Digest from) {
        if (bindings.isEmpty()) {
            log.trace("No bindings to update: {} on: {}", from, member.getId());
            return;
        }

        log.trace("Events to update: {} on: {}", bindings.size(), member.getId());
        for (var bound : bindings) {
            var hash = algorithm.digest(bound.getKey());
            var existing = digests.get(hash);
            var digest = algorithm.digest(bound.toByteString());
            if (existing != null && existing.equals(digest)) {
                continue;
            }
            var states = pending.computeIfAbsent(digest, k -> new CopyOnWriteArrayList<>());
            var found = false;
            for (var cs : states) {
                if (cs.test(bound, from)) {
                    found = true;
                    if (cs.count() >= context.majority()) {
                        add(hash, bound, digest);
                        pending.remove(digest);
                    }
                    break;
                }
            }
            if (!found) {
                states.add(new ConsensusState(bound, from));
            }
        }
    }

    private <B> boolean write(CompletableFuture<B> result, HashMultiset<B> gathered, AtomicInteger tally,
                              Optional<B> futureSailor, Digest hash, Supplier<Boolean> isTimedOut,
                              RingCommunications.Destination<Member, BinderClient> destination) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        var content = futureSailor.get();
        if (content != null) {
            log.debug("Bind: {} from: {}  on: {}", hash, destination.member().getId(), member.getId());
            gathered.add(content);
            var max = max(gathered);
            if (max != null) {
                tally.set(max.getCount());
                if (max.getCount() > context.toleranceLevel()) {
                    result.complete(max.getElement());
                    log.debug("Majority Bind : {} achieved: {} on: {}", max.getCount(), hash, member.getId());
                    return true;
                }
            }
            return !isTimedOut.get();
        } else {
            log.debug("Failed: Bind : {} from: {}  on: {}", hash, destination.member().getId(), member.getId());
            return !isTimedOut.get();
        }
    }

    public interface OpValidator {
        boolean validateBind(Bound bound);

        boolean validateGet(byte[] key);

        boolean validateUnbind(byte[] key);
    }

    private static class ConsensusState {
        private final Bound        binding;
        private final List<Digest> members = new ArrayList<>();

        ConsensusState(Bound binding, Digest from) {
            this.binding = binding;
            members.add(from);
        }

        int count() {
            return members.size();
        }

        /**
         * Test the binding against the receiver's.  If the from id is not already in the members set, add it
         *
         * @param binding - the replicated Bound
         * @param from    - the Digest id of the originating member
         * @return true if the binding equals the receiver's binding, false if not
         */
        boolean test(Bound binding, Digest from) {
            if (!this.binding.equals(binding)) {
                return false;
            }
            for (var m : members) {
                if (m.equals(from)) {
                    return true;
                }
            }
            members.add(from);
            return true;
        }
    }

    private class Reconciled implements ReconciliationService {

        @Override
        public Update reconcile(Intervals intervals, Digest from) {
            var ring = intervals.getRing();
            if (invalid(from, ring)) {
                log.warn("Invalid reconcile from: {} ring: {} on: {}", from, ring, member.getId());
                return Update.getDefaultInstance();
            }
            log.trace("Reconcile from: {} ring: {} on: {}", from, ring, member.getId());
            var builder = LeydenJar.this.reconcile(intervals);
            CombinedIntervals keyIntervals = keyIntervals();
            builder.addAllIntervals(keyIntervals.toIntervals())
                   .setHave(populate(Entropy.nextBitsStreamLong(), keyIntervals));
            log.trace("Reconcile for: {} ring: {} count: {} on: {}", from, ring, builder.getBindingsCount(),
                      member.getId());
            return builder.build();
        }

        @Override
        public void update(Updating update, Digest from) {
            var ring = update.getRing();
            if (invalid(from, ring)) {
                log.warn("Invalid update from: {} ring: {} on: {}", from, ring, member.getId());
                return;
            }
            LeydenJar.this.update(update.getBindingsList(), from);
        }
    }

    private class Borders implements BinderService {

        @Override
        public void bind(Binding request, Digest from) {
            var bound = request.getBound();
            if (!validator.validateBind(bound)) {
                log.warn("Invalid Bind Token on: {}", member.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            var hash = algorithm.digest(bound.getKey());
            log.debug("Bind: {} on: {}", hash, member.getId());
            bottled.put(hash, bound);
            var digest = algorithm.digest(bound.toByteString());
            digests.put(hash, digest);
        }

        @Override
        public Bound get(Key request, Digest from) {
            if (!validator.validateGet(request.getKey().toByteArray())) {
                log.warn("Invalid Get Token on: {}", member.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            var hash = algorithm.digest(request.getKey());
            var bound = bottled.getOrDefault(hash, Bound.getDefaultInstance());
            log.debug("Get: {} bound: {} on: {}", hash, bound != null, member.getId());
            return bound;
        }

        @Override
        public void unbind(Key request, Digest from) {
            if (!validator.validateUnbind(request.getKey().toByteArray())) {
                log.warn("Invalid Unbind Token on: {}", member.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            var hash = algorithm.digest(request.getKey());
            log.debug("Remove: {} on: {}", hash, member.getId());
            bottled.remove(hash);
            digests.remove(hash);
        }
    }
}
