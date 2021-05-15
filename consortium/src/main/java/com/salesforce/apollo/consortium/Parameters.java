/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.io.File;
import java.security.Signature;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.base.Supplier;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.HashKey;

public class Parameters {
    public static class Builder {
        private int                                            checkpointBlockSize   = 8192;
        private Function<Long, File>                           checkpointer          = c -> {
                                                                                         throw new IllegalStateException(
                                                                                                 "No checkpointer defined");
                                                                                     };
        private Router                                         communications;
        private BiFunction<CertifiedBlock, Future<?>, HashKey> consensus;
        private Context<Member>                                context;
        private int                                            deltaCheckpointBlocks = 500;
        private Executor                                       dispatcher            = ForkJoinPool.commonPool();
        private TransactionExecutor                            executor              = (bh, et, c) -> {
                                                                                     };
        private Message                                        genesisData;
        private HashKey                                        genesisViewId;
        private Duration                                       gossipDuration;
        private Duration                                       initialViewTimeout    = Duration.ofSeconds(60);
        private Duration                                       joinTimeout           = Duration.ofMillis(500);
        private int                                            maxBatchByteSize      = 4 * 1024;
        private Duration                                       maxBatchDelay         = Duration.ofMillis(200);
        private int                                            maxBatchSize          = 10;
        private int                                            maxCheckpointBlocks   = DEFAULT_MAX_BLOCKS;
        private int                                            maxCheckpointSegments = DEFAULT_MAX_SEGMENTS;
        private int                                            maxSyncBlocks         = 10;
        private int                                            maxViewBlocks         = 100;
        private Member                                         member;
        private Messenger.Parameters                           msgParameters;
        private int                                            processedBufferSize   = 1000;
        private ScheduledExecutorService                       scheduler;
        private Supplier<Signature>                            signature;
        private File                                           storeFile;
        private Duration                                       submitTimeout         = Duration.ofSeconds(30);
        private Duration                                       synchonrizeDuration   = Duration.ofMillis(500);
        private int                                            synchronizeSlice      = 5;
        private Duration                                       viewTimeout           = Duration.ofSeconds(60);

        public Parameters build() {
            return new Parameters(context, communications, member, msgParameters, scheduler, signature, gossipDuration,
                    consensus, maxBatchSize, maxBatchByteSize, maxBatchDelay, joinTimeout, maxCheckpointSegments,
                    viewTimeout, submitTimeout, processedBufferSize, genesisData, genesisViewId, maxCheckpointBlocks,
                    executor, checkpointer, deltaCheckpointBlocks, storeFile, checkpointBlockSize, initialViewTimeout,
                    dispatcher, synchonrizeDuration, maxViewBlocks, maxSyncBlocks, synchronizeSlice);
        }

        public int getCheckpointBlockSize() {
            return checkpointBlockSize;
        }

        public Function<Long, File> getCheckpointer() {
            return checkpointer;
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

        public int getDeltaCheckpointBlocks() {
            return deltaCheckpointBlocks;
        }

        public Executor getDispatcher() {
            return dispatcher;
        }

        public TransactionExecutor getExecutor() {
            return executor;
        }

        public Message getGenesisData() {
            return genesisData;
        }

        public HashKey getGenesisViewId() {
            return genesisViewId;
        }

        public Duration getGossipDuration() {
            return gossipDuration;
        }

        public Duration getInitialViewTimeout() {
            return initialViewTimeout;
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

        public int getMaxCheckpointBlocks() {
            return maxCheckpointBlocks;
        }

        public int getMaxCheckpointSegments() {
            return maxCheckpointSegments;
        }

        public int getMaxSyncBlocks() {
            return maxSyncBlocks;
        }

        public int getMaxViewBlocks() {
            return maxViewBlocks;
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

        public File getStoreFile() {
            return storeFile;
        }

        public Duration getSubmitTimeout() {
            return submitTimeout;
        }

        public Duration getSynchonrizeDuration() {
            return synchonrizeDuration;
        }

        public int getSynchronizeSlice() {
            return synchronizeSlice;
        }

        public Duration getTransactonTimeout() {
            return submitTimeout;
        }

        public Duration getViewTimeout() {
            return viewTimeout;
        }

        public Builder setCheckpointBlockSize(int checkpointBlockSize) {
            this.checkpointBlockSize = checkpointBlockSize;
            return this;
        }

        public Builder setCheckpointer(Function<Long, File> checkpointer) {
            this.checkpointer = checkpointer;
            return this;
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

        public Builder setDeltaCheckpointBlocks(int deltaCheckpointBlocks) {
            this.deltaCheckpointBlocks = deltaCheckpointBlocks;
            return this;
        }

        public Builder setDispatcher(Executor dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        public Builder setExecutor(TransactionExecutor executor) {
            this.executor = executor;
            return this;
        }

        public Builder setGenesisData(Message genesisData) {
            this.genesisData = genesisData;
            return this;
        }

        public Builder setGenesisViewId(HashKey genesisViewId) {
            this.genesisViewId = genesisViewId;
            return this;
        }

        public Parameters.Builder setGossipDuration(Duration gossipDuration) {
            this.gossipDuration = gossipDuration;
            return this;
        }

        public Builder setInitialViewTimeout(Duration initialViewTimeout) {
            this.initialViewTimeout = initialViewTimeout;
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

        public Builder setMaxCheckpointBlocks(int maxCheckpointBlocks) {
            this.maxCheckpointBlocks = maxCheckpointBlocks;
            return this;
        }

        public Builder setMaxCheckpointSegments(int maxCheckpointSegments) {
            this.maxCheckpointSegments = maxCheckpointSegments;
            return this;
        }

        public Builder setMaxSyncBlocks(int maxSyncBlocks) {
            this.maxSyncBlocks = maxSyncBlocks;
            return this;
        }

        public Builder setMaxViewBlocks(int maxViewBlocks) {
            this.maxViewBlocks = maxViewBlocks;
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

        public Builder setStoreFile(File storeFile) {
            this.storeFile = storeFile;
            return this;
        }

        public Builder setSubmitTimeout(Duration submitTimeout) {
            this.submitTimeout = submitTimeout;
            return this;
        }

        public Builder setSynchonrizeDuration(Duration synchonrizeDuration) {
            this.synchonrizeDuration = synchonrizeDuration;
            return this;
        }

        public Builder setSynchronizeSlice(int synchronizeSlice) {
            this.synchronizeSlice = synchronizeSlice;
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

    public static final int DEFAULT_MAX_BLOCKS   = 200;
    public static final int DEFAULT_MAX_SEGMENTS = 200;

    public static Parameters.Builder newBuilder() {
        return new Builder();
    }

    public final int                                            checkpointBlockSize;
    public final Function<Long, File>                           checkpointer;
    public final Router                                         communications;
    public final BiFunction<CertifiedBlock, Future<?>, HashKey> consensus;
    public final Context<Member>                                context;
    public final int                                            deltaCheckpointBlocks;
    public final Executor                                       dispatcher;
    public final TransactionExecutor                            executor;
    public final Message                                        genesisData;
    public final HashKey                                        genesisViewId;
    public final Duration                                       gossipDuration;
    public final Duration                                       initialViewTimeout;
    public final Duration                                       joinTimeout;
    public final int                                            maxBatchByteSize;
    public final Duration                                       maxBatchDelay;
    public final int                                            maxBatchSize;
    public final int                                            maxCheckpointBlocks;
    public final int                                            maxCheckpointSegments;
    public final int                                            maxSyncBlocks;
    public final int                                            maxViewBlocks;
    public final Member                                         member;
    public final Messenger.Parameters                           msgParameters;
    public final int                                            processedBufferSize;
    public final ScheduledExecutorService                       scheduler;
    public final Supplier<Signature>                            signature;
    public final File                                           storeFile;
    public final Duration                                       submitTimeout;
    public final Duration                                       synchronizeDuration;
    public final int                                            synchronizeSlice;
    public final Duration                                       viewTimeout;

    public Parameters(Context<Member> context, Router communications, Member member, Messenger.Parameters msgParameters,
            ScheduledExecutorService scheduler, Supplier<Signature> signature, Duration gossipDuration,
            BiFunction<CertifiedBlock, Future<?>, HashKey> consensus, int maxBatchSize, int maxBatchByteSize,
            Duration maxBatchDelay, Duration joinTimeout, int maxCheckpointSegments, Duration viewTimeout,
            Duration submitTimeout, int processedBufferSize, Message genesisData, HashKey genesisViewId,
            int maxCheckpointBlocks, TransactionExecutor executor, Function<Long, File> checkpointer,
            int deltaCheckpointBlocks, File storeFile, int checkpointBlockSize, Duration initialViewTimeout,
            Executor dispatcher, Duration synchronizeDuration, int maxViewBlocks, int maxSyncBlocks,
            int synchronizeSlice) {
        this.context = context;
        this.communications = communications;
        this.maxSyncBlocks = maxSyncBlocks;
        this.maxViewBlocks = maxViewBlocks;
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
        this.checkpointer = checkpointer;
        this.storeFile = storeFile;
        this.genesisViewId = genesisViewId;
        this.maxCheckpointBlocks = maxCheckpointBlocks;
        this.maxCheckpointSegments = maxCheckpointSegments;
        this.checkpointBlockSize = checkpointBlockSize;
        this.deltaCheckpointBlocks = deltaCheckpointBlocks;
        this.initialViewTimeout = initialViewTimeout;
        this.dispatcher = dispatcher;
        this.synchronizeDuration = synchronizeDuration;
        this.synchronizeSlice = synchronizeSlice;
    }
}
