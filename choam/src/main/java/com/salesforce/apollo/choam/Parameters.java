/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesforce.apollo.choam.support.CheckpointState;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;

/**
 * @author hal.hildebrand
 *
 */
public record Parameters(Context<Member> context, Router communications, SigningMember member,
                         ReliableBroadcaster.Parameters.Builder combineParameters, ScheduledExecutorService scheduler,
                         Duration gossipDuration, int maxBatchSize, int maxBatchByteSize, int maxCheckpointSegments,
                         Duration submitTimeout, int processedBufferSize, List<ExecutedTransaction> genesisData,
                         Digest genesisViewId, int maxCheckpointBlocks, Consumer<HashedBlock> processor,
                         Function<Long, File> checkpointer, int deltaCheckpointBlocks, File storeFile,
                         int checkpointBlockSize, Executor dispatcher, BiConsumer<Long, CheckpointState> restorer,
                         DigestAlgorithm digestAlgorithm, ReliableBroadcaster.Parameters.Builder coordination,
                         Config.Builder ethereal, int lifetime, ChoamMetrics metrics,
                         SignatureAlgorithm viewSigAlgorithm) {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private int                                    checkpointBlockSize   = 8192;
        private Function<Long, File>                   checkpointer          = c -> {
                                                                                 throw new IllegalStateException("No checkpointer defined");
                                                                             };
        private ReliableBroadcaster.Parameters.Builder combineParams         = ReliableBroadcaster.Parameters.newBuilder();
        private Router                                 communications;
        private Context<Member>                        context;
        private ReliableBroadcaster.Parameters.Builder coordination          = ReliableBroadcaster.Parameters.newBuilder();
        private int                                    deltaCheckpointBlocks = 500;
        private DigestAlgorithm                        digestAlgorithm       = DigestAlgorithm.DEFAULT;
        private Executor                               dispatcher            = ForkJoinPool.commonPool();
        private Config.Builder                         ethereal              = Config.deterministic();
        private List<ExecutedTransaction>              genesisData           = new ArrayList<>();
        private Digest                                 genesisViewId;
        private Duration                               gossipDuration;
        private int                                    lifetime              = 100;
        private int                                    maxBatchByteSize      = 4 * 1024;
        private int                                    maxBatchSize          = 10;
        private int                                    maxCheckpointBlocks   = DEFAULT_MAX_BLOCKS;
        private int                                    maxCheckpointSegments = DEFAULT_MAX_SEGMENTS;
        private SigningMember                          member;
        private ChoamMetrics                           metrics;
        private int                                    processedBufferSize   = 1000;
        private Consumer<HashedBlock>                  processor             = block -> {
                                                                             };
        private BiConsumer<Long, CheckpointState>      restorer              = (height, checkpointState) -> {
                                                                             };
        private ScheduledExecutorService               scheduler;
        private File                                   storeFile;
        private Duration                               submitTimeout         = Duration.ofSeconds(30);
        private SignatureAlgorithm                     viewSigAlgorithm      = SignatureAlgorithm.DEFAULT;

        public Parameters build() {
            return new Parameters(context, communications, member, combineParams, scheduler, gossipDuration,
                                  maxBatchSize, maxBatchByteSize, maxCheckpointSegments, submitTimeout,
                                  processedBufferSize, genesisData, genesisViewId, maxCheckpointBlocks, processor,
                                  checkpointer, deltaCheckpointBlocks, storeFile, checkpointBlockSize, dispatcher,
                                  restorer, digestAlgorithm, coordination, ethereal, lifetime, metrics,
                                  viewSigAlgorithm);
        }

        public int getCheckpointBlockSize() {
            return checkpointBlockSize;
        }

        public Function<Long, File> getCheckpointer() {
            return checkpointer;
        }

        public ReliableBroadcaster.Parameters.Builder getCombineParams() {
            return combineParams;
        }

        public Router getCommunications() {
            return communications;
        }

        public Context<Member> getContext() {
            return context;
        }

        public ReliableBroadcaster.Parameters.Builder getCoordination() {
            return coordination;
        }

        public int getDeltaCheckpointBlocks() {
            return deltaCheckpointBlocks;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public Executor getDispatcher() {
            return dispatcher;
        }

        public Config.Builder getEthereal() {
            return ethereal;
        }

        public List<ExecutedTransaction> getGenesisData() {
            return genesisData;
        }

        public Digest getGenesisViewId() {
            return genesisViewId;
        }

        public Duration getGossipDuration() {
            return gossipDuration;
        }

        public int getLifetime() {
            return lifetime;
        }

        public int getMaxBatchByteSize() {
            return maxBatchByteSize;
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

        public SigningMember getMember() {
            return member;
        }

        public ChoamMetrics getMetrics() {
            return metrics;
        }

        public int getProcessedBufferSize() {
            return processedBufferSize;
        }

        public Consumer<HashedBlock> getProcessor() {
            return processor;
        }

        public BiConsumer<Long, CheckpointState> getRestorer() {
            return restorer;
        }

        public ScheduledExecutorService getScheduler() {
            return scheduler;
        }

        public File getStoreFile() {
            return storeFile;
        }

        public Duration getSubmitTimeout() {
            return submitTimeout;
        }

        public Duration getTransactonTimeout() {
            return submitTimeout;
        }

        public SignatureAlgorithm getViewSigAlgorithm() {
            return viewSigAlgorithm;
        }

        public Builder setCheckpointBlockSize(int checkpointBlockSize) {
            this.checkpointBlockSize = checkpointBlockSize;
            return this;
        }

        public Builder setCheckpointer(Function<Long, File> checkpointer) {
            this.checkpointer = checkpointer;
            return this;
        }

        public Builder setCombineParams(ReliableBroadcaster.Parameters.Builder combineParams) {
            this.combineParams = combineParams;
            return this;
        }

        public Builder setCommunications(Router communications) {
            this.communications = communications;
            return this;
        }

        @SuppressWarnings("unchecked")
        public Parameters.Builder setContext(Context<? extends Member> context) {
            this.context = (Context<Member>) context;
            return this;
        }

        public Builder setCoordination(ReliableBroadcaster.Parameters.Builder coordination) {
            this.coordination = coordination;
            return this;
        }

        public Builder setDeltaCheckpointBlocks(int deltaCheckpointBlocks) {
            this.deltaCheckpointBlocks = deltaCheckpointBlocks;
            return this;
        }

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public Builder setDispatcher(Executor dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        public Builder setEthereal(Config.Builder ethereal) {
            this.ethereal = ethereal;
            return this;
        }

        public Builder setGenesisData(List<ExecutedTransaction> genesisData) {
            this.genesisData = genesisData;
            return this;
        }

        public Builder setGenesisViewId(Digest genesisViewId) {
            this.genesisViewId = genesisViewId;
            return this;
        }

        public Parameters.Builder setGossipDuration(Duration gossipDuration) {
            this.gossipDuration = gossipDuration;
            return this;
        }

        public Builder setLifetime(int lifetime) {
            this.lifetime = lifetime;
            return this;
        }

        public Builder setMaxBatchByteSize(int maxBatchByteSize) {
            this.maxBatchByteSize = maxBatchByteSize;
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

        public Parameters.Builder setMember(SigningMember member) {
            this.member = member;
            return this;
        }

        public Builder setMetrics(ChoamMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder setProcessedBufferSize(int processedBufferSize) {
            this.processedBufferSize = processedBufferSize;
            return this;
        }

        public Builder setProcessor(Consumer<HashedBlock> processor) {
            this.processor = processor;
            return this;
        }

        public Builder setRestorer(BiConsumer<Long, CheckpointState> biConsumer) {
            this.restorer = biConsumer;
            return this;
        }

        public Parameters.Builder setScheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
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

        public Builder setTransactonTimeout(Duration transactonTimeout) {
            this.submitTimeout = transactonTimeout;
            return this;
        }

        public Builder setViewSigAlgorithm(SignatureAlgorithm viewSigAlgorithm) {
            this.viewSigAlgorithm = viewSigAlgorithm;
            return this;
        }

    }

    public static final int DEFAULT_MAX_BLOCKS = 200;
    public static final int DEFAULT_MAX_SEGMENTS = 200;

}
