/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.google.common.collect.Sets;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.slush.Communications.Sink;
import com.salesforce.apollo.slush.config.Parameters;

/**
 * Common abstraction for Avalanche protocols.
 * 
 * @author hal.hildebrand
 * @since 220
 */
abstract public class AbstractProtocol<Params extends Parameters, T> {

    /**
     * The Sink for the protocol
     */
    protected class ProtocolSink implements Sink<T> {
        protected final UUID id = GENERATOR.generate();
        protected final Set<Member> previous;
        protected final HashSet<Member> replied;
        protected final Map<T, Integer> responses;
        protected final CompletableFuture<T> result;
        protected final int retries;

        protected ProtocolSink(int retries, Set<Member> sample, CompletableFuture<T> result, Set<Member> previous) {
            assert sample.size() > threshold : "Sample size (" + sample.size() + ") must be > " + threshold;
            this.result = result;
            this.previous = previous;
            this.retries = retries;
            this.previous.addAll(sample);
            replied = new HashSet<>();
            replied.addAll(sample);
            responses = new HashMap<>();
        }

        @Override
        public void consume(Member from, T response) {
            log.trace("{} consume {} from {}", communications.address(), response, from);

            // Only care if it's actually a new reply
            if (replied.remove(from)) {
                responses.put(response, responses.computeIfAbsent(response, k -> 0) + 1);
            }

            if (replied.isEmpty()) { // we're done, Jim
                communications.cancel(id);
                T decision = decide(responses);
                log.debug("{} decide: {}", communications.address(), decision);
                if (decision != null) {
                    setCurrent(decision);
                }
                result.complete(decision);
            }
        }

        @Override
        public UUID getId() {
            return id;
        }

        @Override
        public void onTimeout() {
            if (retries > 0) {
                try {
                    // the retry continuation for the current round
                    query(result, previous, retries - 1);
                } catch (Throwable t) {
                    log.error(communications.address() + " error in protocol", t);
                }
            } else {
                // We're done, Jim
                result.completeExceptionally(new TimeoutException(String.format("failed to receive %s responses after %s milliseconds",
                                                                                parameters.sample, parameters.retries
                                                                                        * (parameters.unit.toMillis(parameters.timeout)))));
            }
        }
    }

    protected final static TimeBasedGenerator GENERATOR = Generators.timeBasedGenerator();
    private final static Logger log = LoggerFactory.getLogger(AbstractProtocol.class);

    /**
     * Uniformly sample a population of addresses using the supplied entropy
     * 
     * @param population
     * @param sampleSize
     * @param from
     * @param entropy
     * @return A subset of the supplied population, uniformly sampled.
     */
    public static Set<Member> sample(Collection<Member> population, int sampleSize, Member from, Random entropy) {
        assert sampleSize <= population.size() : "Cannot take a sample size " + sampleSize
                + " from a population of size " + population.size() + " members";

        // Due to the insanity of non indexed sets for any useful cardinalities or dynamism, we have to
        // first determine indexes and then iterate using increments to find the random indexes determined.
        TreeSet<Integer> selected = new TreeSet<>();

        // keep going until we have a full subset
        while (selected.size() < sampleSize) {
            selected.add(entropy.nextInt(population.size()));
        }

        // convert selected indexes into Addresses
        Set<Member> subset = new HashSet<>(sampleSize);
        int i = 0;
        int current = selected.first();
        for (Member member : population) {
            if (current == i) {
                subset.add(member);
                selected.remove(current);
                if (selected.isEmpty()) {
                    break;
                }
                current = selected.first();
            }
            i++;
        }
        return subset;
    }

    protected final Communications communications;
    protected final Random entropy;
    protected final Collection<T> enumerated;
    protected final AtomicReference<CompletableFuture<T>> futureSailor = new AtomicReference<>();
    protected final Params parameters;
    protected final ScheduledExecutorService scheduler;

    protected final int threshold;

    private volatile T current;

    public AbstractProtocol(T initialValue, Collection<T> enumerated, Communications communications, Params parameters,
            Random entropy, ScheduledExecutorService scheduler) {
        this.communications = communications;
        this.entropy = entropy;
        this.parameters = parameters;
        threshold = (int)(parameters.alpha * parameters.sample);
        this.scheduler = scheduler;
        this.enumerated = enumerated;
        setCurrent(initialValue);
    }

    public T finalizeChoice() {
        if (futureSailor.get() == null) {
            T value = getCurrent();
            if (value == null) {
                throw new IllegalStateException("No value determined, loop has not been initiated for "
                        + communications.address());
            }
            return value;
        }
        try {
            return futureSailor.get().get();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Finalize interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Unexpected exception in finalize", e);
        }
    }

    public T getCurrent() {
        return current;
    }

    /**
     * Determine the choice of the reciever
     * 
     * @return a promise that determines the final choice of the reciever, or an exception indicating the problem
     */
    public CompletableFuture<T> loop() {
        if (futureSailor.compareAndSet(null, new CompletableFuture<>())) {
            queryRound();
        }
        return futureSailor.get();
    }

    /**
     * The fundamental query of the protocol.
     * 
     * @param theirValue
     *            - the value of the querying member
     * @return - the reciver's current value if non null, the computed initial value using theirValue as the default (if
     *         no opinion)
     */
    public T query(T theirValue) {
        log.trace("{} query, their value: {}", communications.address(), theirValue);
        T myValue = getCurrent();
        if (myValue == null) {
            initialValue(theirValue);
            loop();
        }
        return getCurrent();
    }

    protected Supplier<T> constructQuery() {
        return new Query<T>(getCurrent());
    }

    /**
     * Continuation of the loop
     * 
     * @param round
     */
    protected void continueLoop() {
        log.debug("{} continuing loop", communications.address());
        scheduler.schedule(() -> {
            try {
                queryRound();
            } catch (Throwable t) {
                log.error(communications.address() + " Error in protocol", t);
            }
        }, parameters.interval, parameters.intervalUnit);
    }

    protected AbstractProtocol<Params, T>.ProtocolSink createSink(int retries, Set<Member> sample,
            CompletableFuture<T> result, Set<Member> previous) {
        return new ProtocolSink(retries, sample, result, previous);
    }

    /**
     * the protocol's decision function
     * 
     * @param responses
     *            - the gathered responses from our query sample
     */
    abstract protected T decide(Map<T, Integer> responses);

    /**
     * Compute the initial value of the receiver. The default is to use the supplied value from the query sender
     * 
     * @param theirValue
     */
    protected void initialValue(T theirValue) {
        setCurrent(theirValue);
    }

    /**
     * Asynchronously perform one query round
     * 
     * @return the promise of the value determination of this round
     */
    protected CompletableFuture<T> query() {
        T myValue = getCurrent();
        if (myValue == null) {
            log.trace("{} value not set", communications.address());
            return null;
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        HashSet<Member> previous = new HashSet<>();
        previous.add(communications.address()); // We don't query ourselves
        query(result, previous, parameters.retries);
        return result;
    }

    /**
     * The query primitive. Broadcast a query to a select a sample from the total population
     * 
     * @param result
     *            - our promise of a value determination
     * @param previous
     *            - the previously contacted members of the network, if retrying
     * @param retries
     *            - the number of retries left
     */
    protected void query(CompletableFuture<T> result, Set<Member> previous, int retries) {
        // Consider only the population we have not previously considered
        Collection<Member> considered = Sets.difference(communications.getPopulation(), previous);

        // Fallback to the total population, if not enough members for consideration.
        if (considered.size() < parameters.sample) {
            log.debug("{} fallback to total population", communications.address());
            HashSet<Member> self = new HashSet<>();
            self.add(communications.address()); // We don't query ourselves
            considered = Sets.difference(communications.getPopulation(), self);
        }

        Set<Member> sample = sample(considered, parameters.sample, communications.address(), entropy);

        ProtocolSink sink = createSink(retries, sample, result, previous);
        if (log.isTraceEnabled()) {
            log.trace("{} sink: {} sample: {} value: {}", communications.address(), sink.getId(),
                      sample.stream().map(a -> a).collect(Collectors.toList()), getCurrent());
        }
        communications.await(sink, parameters.timeout, parameters.unit).query(constructQuery(), sample, sink.getId());
    }

    /**
     * perform one round of querying for the protocol
     * 
     * @param futureSailor
     *            - the promise of the future value determination result
     */
    protected void queryRound() {
        if (futureSailor.get().isDone()) {
            return;
        }
        log.debug("{} query loop, value: {}", communications.address(), getCurrent());
        CompletableFuture<T> current = query();

        scheduler.execute(() -> {
            if (current == null) {
                continueLoop();
                return;
            }
            log.debug("{} starting protocol loop, initial value: {}", communications.address(), getCurrent());
            try {
                T currentValue;
                try {
                    currentValue = current.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    log.error(String.format("%s error: %s", communications.address(), cause.getMessage()), cause);
                    futureSailor.get().completeExceptionally(cause);
                    return;
                } catch (InterruptedException e) {
                    log.error(String.format("%s interrupted", communications.address()), e);
                    futureSailor.get().completeExceptionally(e);
                    return;
                }
                log.debug("{} value: {}", communications.address(), currentValue);
                if (currentValue != null && !shouldContinue()) {
                    log.info("{} final value: {}", communications.address(), currentValue);
                    futureSailor.get().complete(currentValue);
                    return;
                }
                continueLoop();
            } catch (Throwable e) {
                log.error(communications.address() + " protocol failure", e);
            }
        });
    }

    protected void setCurrent(T value) {
        this.current = value;
    }

    /**
     * @return true if the querying should continue
     */
    protected abstract boolean shouldContinue();
}
