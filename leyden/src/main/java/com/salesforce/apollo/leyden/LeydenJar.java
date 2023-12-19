package com.salesforce.apollo.leyden;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.protobuf.InvalidProtocolBufferException;
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
import com.salesforce.apollo.stereotomy.event.proto.Attachment;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Hex;
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
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    private final        DigestAlgorithm                                                              algorithm;
    private final        double                                                                       fpr;
    private final        SigningMember                                                                member;
    private final        MVMap<byte[], Bound>                                                         bottled;
    private final        AtomicBoolean                                                                started    = new AtomicBoolean();
    private final        RingCommunications<Member, ReconciliationClient>                             reconcile;
    private final        NavigableMap<Digest, List<ConsensusState>>                                   pending    = new ConcurrentSkipListMap<>();
    private final        Borders                                                                      borders;
    private final        Reconciled                                                                   recon;
    private final        TemporalAmount                                                               operationTimeout;
    private final        Duration                                                                     operationsFrequency;
    private final        ScheduledExecutorService                                                     scheduler  = Executors.newScheduledThreadPool(
    1, Thread.ofVirtual().factory());
    private final        OpValidator                                                                  validator;

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
        bottled = store.openMap(LEYDEN_JAR,
                                new MVMap.Builder<byte[], Bound>().valueType(new ProtobufDatatype<Bound>(b -> {
                                    try {
                                        return Bound.parseFrom(b);
                                    } catch (InvalidProtocolBufferException e) {
                                        throw new IllegalArgumentException(e);
                                    }
                                })));
        reconcile = new RingCommunications<>(this.context, member, reconComms);
    }

    public void bindRequest(Bound bound) {

    }

    public Bound get(KeyAndToken key) {
        log.info("get: {} on: {}", Hex.hex(key.toByteArray()), member.getId());
        var hash = algorithm.digest(key.toByteString());
        Instant timedOut = Instant.now().plus(operationTimeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        var result = new CompletableFuture<Bound>();
        var gathered = HashMultiset.<Bound>create();
        var iterate = new RingIterator<Member, BinderClient>(operationsFrequency, context, member, scheduler,
                                                             binderComms);
        iterate.noDuplicates()
               .iterate(hash, null, (link, r) -> link.get(key), () -> failedMajority(result, maxCount(gathered)),
                        (tally, futureSailor, destination) -> read(result, gathered, tally, futureSailor, hash,
                                                                   isTimedOut, destination, "get attachment",
                                                                   Attachment.getDefaultInstance()),
                        t -> failedMajority(result, maxCount(gathered)));
        try {
            return result.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    public void start(Duration gossip) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        log.info("Starting: {}", member.getId());
        reconcile(scheduler, gossip);
        binderComms.register(context.getId(), borders);
        reconComms.register(context.getId(), recon);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping: {}", member.getId());
        binderComms.deregister(context.getId());
        reconComms.deregister(context.getId());
    }

    public void unbind(KeyAndToken key) {

    }

    private void add(Bound bound) {
        var hash = algorithm.digest(bound.getKey());
        bottled.put(hash.getBytes(), bound);
        log.info("Add: {} on: {}", Hex.hex(bound.getKey().toByteArray()), member.getId());
    }

    private void bindRequest(Binding request) {
    }

    private Bound binding(Digest d) {
        return bottled.get(d.getBytes());
    }

    private Stream<Digest> bindingsIn(KeyInterval i) {
        Iterator<Digest> it = new Iterator<Digest>() {
            private final Iterator<byte[]> iterate = bottled.keyIterator(i.getBegin().getBytes());
            private Digest next;

            {
                if (iterate.hasNext()) {
                    next = new Digest(algorithm, iterate.next());
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
                if (returned == null) {
                    throw new NoSuchElementException();
                }
                if (iterate.hasNext()) {
                    next = new Digest(algorithm, iterate.next());
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
        "Unable to achieve majority read, max: %s" + " required: %s on: %s".formatted(maxAgree, context.majority(),
                                                                                      member.getId())));
    }

    private Bound getRequest(KeyAndToken request) {
        return null;
    }

    private boolean inValid(Digest from, int ring) {
        if (ring >= context.getRingCount() || ring < 0) {
            log.warn("invalid ring {} from {} on: {}", ring, from, member.getId());
            return true;
        }
        Member fromMember = context.getMember(from);
        if (fromMember == null) {
            return true;
        }
        Member successor = context.ring(ring).successor(fromMember, m -> context.isActive(m.getId()));
        if (successor == null) {
            return true;
        }
        return !successor.equals(member);
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
        bottled.keyIterator(algorithm.getOrigin().getBytes()).forEachRemaining(b -> {
            var d = new Digest(algorithm, b);
            if (keyIntervals.test(d)) {
                bff.add(d);
            }
        });
        return bff.toBff();
    }

    private boolean read(CompletableFuture<Bound> result, HashMultiset<Bound> gathered, AtomicInteger tally,
                         Optional<Bound> futureSailor, Digest hash, Supplier<Boolean> isTimedOut,
                         RingCommunications.Destination<Member, BinderClient> destination, String getAttachment,
                         Attachment defaultInstance) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        Bound content = futureSailor.get();
        if (content != null) {
            log.trace("bound: {} from: {}  on: {}", hash, destination.member().getId(), member.getId());
            gathered.add(content);
            var max = max(gathered);
            if (max != null) {
                tally.set(max.getCount());
                if (max.getCount() > context.toleranceLevel()) {
                    result.complete(max.getElement());
                    log.debug("Majority: {} achieved: {} on: {}", max.getCount(), hash, member.getId());
                    return false;
                }
            }
            return !isTimedOut.get();
        } else {
            log.debug("Failed {} from: {}  on: {}", hash, destination.member().getId(), member.getId());
            return !isTimedOut.get();
        }
    }

    private Update reconcile(ReconciliationClient link, Integer ring) {
        if (member.equals(link.getMember())) {
            return null;
        }
        CombinedIntervals keyIntervals = keyIntervals();
        log.trace("Interval reconciliation on ring: {} with: {} on: {} intervals: {}", ring, link.getMember(),
                  member.getId(), keyIntervals);
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
            scheduler.schedule(() -> reconcile(scheduler, duration), duration.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private void reconcile(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        reconcile.execute(this::reconcile,
                          (futureSailor, destination) -> reconcile(futureSailor, destination, scheduler, duration));

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
                 .peek(d -> log.trace("reconcile digest: {} on: {}", d, member.getId()))
                 .filter(d -> !biff.contains(d))
                 .peek(d -> log.trace("filtered reconcile digest: {} on: {}", d, member.getId()))
                 .map(this::binding)
                 .filter(Objects::nonNull)
                 .forEach(update::addBindings);
        return update;
    }

    private void unbindRequest(KeyAndToken request) {
    }

    private void update(List<Bound> bindings, Digest from) {
        if (bindings.isEmpty()) {
            log.trace("No bindings to update: {} on: {}", from, member.getId());
            return;
        }

        log.trace("Events to update: {} on: {}", bindings.size(), member.getId());
        for (var bound : bindings) {
            var digest = algorithm.digest(bound.toByteString());
            var states = pending.computeIfAbsent(digest, k -> new ArrayList<>());
            var found = false;
            for (var cs : states) {
                if (cs.test(bound, from)) {
                    found = true;
                    if (cs.count() >= context.majority()) {
                        add(bound);
                    }
                    break;
                }
            }
            if (!found) {
                states.add(new ConsensusState(bound, from));
            }
        }
    }

    public enum Operation {
        PUT, DELETE, GET;
    }

    public interface OpValidator {
        boolean validateBind(Bound bound, Token token);

        boolean validateGet(byte[] key, Token token);

        boolean validateUnbind(byte[] key, Token token);
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
            if (inValid(from, ring)) {
                log.trace("Invalid reconcile from: {} ring: {} on: {}", from, ring, member.getId());
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
            if (inValid(from, ring)) {
                return;
            }
            LeydenJar.this.update(update.getBindingsList(), from);
        }
    }

    private class Borders implements BinderService {

        @Override
        public void bind(Binding request, Digest from) {
            Member predecessor = context.ring(request.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.debug("Invalid Bind ring on {}:{} from: {} on ring: {} - not predecessor: {}", context.getId(),
                          member, from, request.getRing(), predecessor.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            if (!validator.validateBind(request.getBound(), Token.fromBytes(request.getToken().toByteArray()))) {
                log.warn("Invalid Bind Token on {}:{}", context.getId(), member.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            LeydenJar.this.bindRequest(request);
        }

        @Override
        public Bound get(KeyAndToken request, Digest from) {
            Member predecessor = context.ring(request.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.debug("Invalid Get ring on {}:{} from: {} on ring: {} - not predecessor: {}", context.getId(),
                          member.getId(), from, request.getRing(), predecessor.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            if (!validator.validateGet(request.getKey().toByteArray(),
                                       Token.fromBytes(request.getToken().toByteArray()))) {
                log.warn("Invalid Get Token on {}:{}", context.getId(), member.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            return LeydenJar.this.getRequest(request);
        }

        @Override
        public void unbind(KeyAndToken request, Digest from) {
            Member predecessor = context.ring(request.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.debug("Invalid Unbind ring on {}:{} from: {} on ring: {} - not predecessor: {}", context.getId(),
                          member.getId(), from, request.getRing(), predecessor.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            if (!validator.validateUnbind(request.getKey().toByteArray(),
                                          Token.fromBytes(request.getToken().toByteArray()))) {
                log.warn("Invalid Unbind Token on {}:{}", context.getId(), member.getId());
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            LeydenJar.this.unbindRequest(request);
        }
    }
}
