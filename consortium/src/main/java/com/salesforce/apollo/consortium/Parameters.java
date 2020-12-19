/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.security.Signature;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.google.common.base.Supplier;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.HashKey;

public class Parameters {
    public static class Builder {
        private Router                                                         communications;
        private BiFunction<CertifiedBlock, Future<?>, HashKey>                 consensus;
        private Context<Member>                                                context;
        private BiConsumer<ExecutedTransaction, BiConsumer<Object, Throwable>> executor            = (et, c) -> {
                                                                                                   };
        private byte[]                                                         genesisData         = "Give me food or give me slack or kill me".getBytes();
        private Duration                                                       gossipDuration;
        private Duration                                                       joinTimeout         = Duration.ofMillis(500);
        private int                                                            maxBatchByteSize    = 4 * 1024;
        private Duration                                                       maxBatchDelay       = Duration.ofMillis(200);
        private int                                                            maxBatchSize        = 10;
        private Member                                                         member;
        private Messenger.Parameters                                           msgParameters;
        private int                                                            processedBufferSize = 1000;
        private ScheduledExecutorService                                       scheduler;
        private Supplier<Signature>                                            signature;
        private Duration                                                       submitTimeout       = Duration.ofSeconds(30);
        private Duration                                                       viewTimeout         = Duration.ofSeconds(60);

        public Parameters build() {
            return new Parameters(context, communications, member, msgParameters, scheduler, signature, gossipDuration,
                    consensus, maxBatchSize, maxBatchByteSize, maxBatchDelay, joinTimeout, viewTimeout, submitTimeout,
                    processedBufferSize, genesisData, executor);
        }

        public Router getCommunications() {
            return communications;
        }

        public BiFunction<CertifiedBlock, Future<?>, HashKey> getConsensus() {
            return consensus;
        }

        public Context<Member> getContext() {
            return context;
        }

        public BiConsumer<ExecutedTransaction, BiConsumer<Object, Throwable>> getExecutor() {
            return executor;
        }

        public byte[] getGenesisData() {
            return genesisData;
        }

        public Duration getGossipDuration() {
            return gossipDuration;
        }

        public Duration getJoinTimeout() {
            return joinTimeout;
        }

        public int getMaxBatchByteSize() {
            return maxBatchByteSize;
        }

        public Duration getMaxBatchDelay() {
            return maxBatchDelay;
        }

        public int getMaxBatchSize() {
            return maxBatchSize;
        }

        public Member getMember() {
            return member;
        }

        public Messenger.Parameters getMsgParameters() {
            return msgParameters;
        }

        public int getProcessedBufferSize() {
            return processedBufferSize;
        }

        public ScheduledExecutorService getScheduler() {
            return scheduler;
        }

        public Supplier<Signature> getSignature() {
            return signature;
        }

        public Duration getSubmitTimeout() {
            return submitTimeout;
        }

        public Duration getTransactonTimeout() {
            return submitTimeout;
        }

        public Duration getViewTimeout() {
            return viewTimeout;
        }

        public Builder setCommunications(Router communications) {
            this.communications = communications;
            return this;
        }

        public Builder setConsensus(BiFunction<CertifiedBlock, Future<?>, HashKey> consensus) {
            this.consensus = consensus;
            return this;
        }

        @SuppressWarnings("unchecked")
        public Parameters.Builder setContext(Context<? extends Member> context) {
            this.context = (Context<Member>) context;
            return this;
        }

        public Builder setExecutor(BiConsumer<ExecutedTransaction, BiConsumer<Object, Throwable>> executor) {
            this.executor = executor;
            return this;
        }

        public Builder setGenesisData(byte[] genesisData) {
            this.genesisData = genesisData;
            return this;
        }

        public Parameters.Builder setGossipDuration(Duration gossipDuration) {
            this.gossipDuration = gossipDuration;
            return this;
        }

        public Builder setJoinTimeout(Duration joinTimeout) {
            this.joinTimeout = joinTimeout;
            return this;
        }

        public Builder setMaxBatchByteSize(int maxBatchByteSize) {
            this.maxBatchByteSize = maxBatchByteSize;
            return this;
        }

        public Builder setMaxBatchDelay(Duration maxBatchDelay) {
            this.maxBatchDelay = maxBatchDelay;
            return this;
        }

        public Builder setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Parameters.Builder setMember(Member member) {
            this.member = member;
            return this;
        }

        public Parameters.Builder setMsgParameters(Messenger.Parameters msgParameters) {
            this.msgParameters = msgParameters;
            return this;
        }

        public Builder setProcessedBufferSize(int processedBufferSize) {
            this.processedBufferSize = processedBufferSize;
            return this;
        }

        public Parameters.Builder setScheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Parameters.Builder setSignature(Supplier<Signature> signature) {
            this.signature = signature;
            return this;
        }

        public Builder setSubmitTimeout(Duration submitTimeout) {
            this.submitTimeout = submitTimeout;
            return this;
        }

        public Builder setTransactonTimeout(Duration transactonTimeout) {
            this.submitTimeout = transactonTimeout;
            return this;
        }

        public Builder setViewTimeout(Duration viewTimeout) {
            this.viewTimeout = viewTimeout;
            return this;
        }
    }

    public static Parameters.Builder newBuilder() {
        return new Builder();
    }

    public final Router                                                         communications;
    public final BiFunction<CertifiedBlock, Future<?>, HashKey>                 consensus;
    public final Context<Member>                                                context;
    public final BiConsumer<ExecutedTransaction, BiConsumer<Object, Throwable>> executor;
    public final byte[]                                                         genesisData;
    public final Duration                                                       gossipDuration;
    public final Duration                                                       joinTimeout;
    public final int                                                            maxBatchByteSize;
    public final Duration                                                       maxBatchDelay;
    public final int                                                            maxBatchSize;
    public final Member                                                         member;
    public final Messenger.Parameters                                           msgParameters;
    public final int                                                            processedBufferSize;
    public final ScheduledExecutorService                                       scheduler;
    public final Supplier<Signature>                                            signature;
    public final Duration                                                       submitTimeout;
    public final Duration                                                       viewTimeout;

    public Parameters(Context<Member> context, Router communications, Member member, Messenger.Parameters msgParameters,
            ScheduledExecutorService scheduler, Supplier<Signature> signature, Duration gossipDuration,
            BiFunction<CertifiedBlock, Future<?>, HashKey> consensus, int maxBatchSize, int maxBatchByteSize,
            Duration maxBatchDelay, Duration joinTimeout, Duration viewTimeout, Duration submitTimeout,
            int processedBufferSize, byte[] genesisData,
            BiConsumer<ExecutedTransaction, BiConsumer<Object, Throwable>> executor) {
        this.context = context;
        this.communications = communications;
        this.member = member;
        this.msgParameters = msgParameters;
        this.scheduler = scheduler;
        this.signature = signature;
        this.gossipDuration = gossipDuration;
        this.consensus = consensus;
        this.maxBatchSize = maxBatchSize;
        this.maxBatchByteSize = maxBatchByteSize;
        this.maxBatchDelay = maxBatchDelay;
        this.joinTimeout = joinTimeout;
        this.viewTimeout = viewTimeout;
        this.submitTimeout = submitTimeout;
        this.processedBufferSize = processedBufferSize;
        this.genesisData = genesisData;
        this.executor = executor;
    }
}
