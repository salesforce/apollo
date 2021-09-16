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
import java.util.function.Function;

import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.support.CheckpointState;
import com.salesforce.apollo.choam.support.ChoamMetrics;
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
                         ReliableBroadcaster.Parameters.Builder combine, ScheduledExecutorService scheduler,
                         Duration gossipDuration, int maxCheckpointSegments, Duration submitTimeout,
                         List<Transaction> genesisData, Digest genesisViewId, TransactionExecutor processor,
                         Function<Long, File> checkpointer, File storeFile, int checkpointBlockSize,
                         Executor dispatcher, BiConsumer<Long, CheckpointState> restorer,
                         DigestAlgorithm digestAlgorithm, ChoamMetrics metrics, SignatureAlgorithm viewSigAlgorithm,
                         int synchronizationCycles, Duration synchronizeDuration, int regenerationCycles,
                         Duration synchronizeTimeout, Executor submitDispatcher, int toleranceLevel,
                         BootstrapParameters bootstrap, ProducerParameters producer, int txnPermits) {

    public record BootstrapParameters(Duration gossipDuration, int maxViewBlocks, int maxSyncBlocks) {

        public static Builder newBuilder() {
            return new Builder();
        }
        public static class Builder {
            private Duration gossipDuration = Duration.ofSeconds(1);
            private int      maxSyncBlocks  = 100;
            private int      maxViewBlocks  = 100;

            public BootstrapParameters build() {
                return new BootstrapParameters(gossipDuration, maxViewBlocks, maxSyncBlocks);
            }

            public Duration getGossipDuration() {
                return gossipDuration;
            }

            public int getMaxSyncBlocks() {
                return maxSyncBlocks;
            }

            public int getMaxViewBlocks() {
                return maxViewBlocks;
            }

            public Builder setGossipDuration(Duration gossipDuration) {
                this.gossipDuration = gossipDuration;
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
        }
    }

    public record ProducerParameters(Config.Builder ethereal, Duration gossipDuration,
                                     ReliableBroadcaster.Parameters.Builder coordination, int maxBatchByteSize) {

        public static Builder newBuilder() {
            return new Builder();
        }
        public static class Builder {
            private ReliableBroadcaster.Parameters.Builder coordination     = ReliableBroadcaster.Parameters.newBuilder();
            private Config.Builder                         ethereal         = Config.deterministic();
            private Duration                               gossipDuration   = Duration.ofSeconds(1);
            private int                                    maxBatchByteSize = 256 * 1024;;

            public ProducerParameters build() {
                return new ProducerParameters(ethereal, gossipDuration, coordination, maxBatchByteSize);
            }

            public ReliableBroadcaster.Parameters.Builder getCoordination() {
                return coordination;
            }

            public Config.Builder getEthereal() {
                return ethereal;
            }

            public Duration getGossipDuration() {
                return gossipDuration;
            }

            public int getMaxBatchByteSize() {
                return maxBatchByteSize;
            }

            public Builder setCoordination(ReliableBroadcaster.Parameters.Builder coordination) {
                this.coordination = coordination;
                return this;
            }

            public Builder setEthereal(Config.Builder ethereal) {
                this.ethereal = ethereal;
                return this;
            }

            public Builder setGossipDuration(Duration gossipDuration) {
                this.gossipDuration = gossipDuration;
                return this;
            }

            public Builder setMaxBatchByteSize(int maxBatchByteSize) {
                this.maxBatchByteSize = maxBatchByteSize;
                return this;
            }
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private BootstrapParameters                    bootstrap             = BootstrapParameters.newBuilder().build();
        private int                                    checkpointBlockSize   = 8192;
        private Function<Long, File>                   checkpointer          = c -> {
                                                                                 throw new IllegalStateException("No checkpointer defined");
                                                                             };
        private ReliableBroadcaster.Parameters.Builder combineParams         = ReliableBroadcaster.Parameters.newBuilder();
        private Router                                 communications;
        private Context<Member>                        context;
        private DigestAlgorithm                        digestAlgorithm       = DigestAlgorithm.DEFAULT;
        private Executor                               dispatcher            = ForkJoinPool.commonPool();
        private List<Transaction>                      genesisData           = new ArrayList<>();
        private Digest                                 genesisViewId;
        private Duration                               gossipDuration        = Duration.ofSeconds(1);
        private int                                    maxCheckpointSegments = DEFAULT_MAX_SEGMENTS;
        private SigningMember                          member;
        private ChoamMetrics                           metrics;
        private TransactionExecutor                    processor             = (t, f) -> {
                                                                             };
        private ProducerParameters                     producer              = ProducerParameters.newBuilder().build();
        private int                                    regenerationCycles    = 20;
        private BiConsumer<Long, CheckpointState>      restorer              = (height, checkpointState) -> {
                                                                             };
        private ScheduledExecutorService               scheduler;
        private File                                   storeFile;
        private Executor                               submitDispatcher      = ForkJoinPool.commonPool();
        private Duration                               submitTimeout         = Duration.ofSeconds(30);
        private int                                    synchronizationCycles = 10;
        private Duration                               synchronizeDuration   = Duration.ofMillis(500);
        private Duration                               synchronizeTimeout    = Duration.ofSeconds(30);
        private int                                    txnPermits            = 3_000;
        private SignatureAlgorithm                     viewSigAlgorithm      = SignatureAlgorithm.DEFAULT;

        public Parameters build() {
            final double n = context.getRingCount();
            var toleranceLevel = (int) ((n - 1.0) / 3.0 + 1.0);
            return new Parameters(context, communications, member, combineParams, scheduler, gossipDuration,
                                  maxCheckpointSegments, submitTimeout, genesisData, genesisViewId, processor,
                                  checkpointer, storeFile, checkpointBlockSize, dispatcher, restorer, digestAlgorithm,
                                  metrics, viewSigAlgorithm, synchronizationCycles, synchronizeDuration,
                                  regenerationCycles, synchronizeTimeout, submitDispatcher, toleranceLevel, bootstrap,
                                  producer, txnPermits);
        }

        public BootstrapParameters getBootstrap() {
            return bootstrap;
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

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public Executor getDispatcher() {
            return dispatcher;
        }

        public List<Transaction> getGenesisData() {
            return genesisData;
        }

        public Digest getGenesisViewId() {
            return genesisViewId;
        }

        public Duration getGossipDuration() {
            return gossipDuration;
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

        public TransactionExecutor getProcessor() {
            return processor;
        }

        public ProducerParameters getProducer() {
            return producer;
        }

        public int getRegenerationCycles() {
            return regenerationCycles;
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

        public Executor getSubmitDispatcher() {
            return submitDispatcher;
        }

        public Duration getSubmitTimeout() {
            return submitTimeout;
        }

        public int getSynchronizationCycles() {
            return synchronizationCycles;
        }

        public Duration getSynchronizeDuration() {
            return synchronizeDuration;
        }

        public Duration getSynchronizeTimeout() {
            return synchronizeTimeout;
        }

        public Duration getTransactonTimeout() {
            return submitTimeout;
        }

        public int getTxnPermits() {
            return txnPermits;
        }

        public SignatureAlgorithm getViewSigAlgorithm() {
            return viewSigAlgorithm;
        }

        public Builder setBootstrap(BootstrapParameters bootstrap) {
            this.bootstrap = bootstrap;
            return this;
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

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public Builder setDispatcher(Executor dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        public Builder setGenesisData(List<Transaction> genesisData) {
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

        public Builder setProcessor(TransactionExecutor processor) {
            this.processor = processor;
            return this;
        }

        public Builder setProducer(ProducerParameters producer) {
            this.producer = producer;
            return this;
        }

        public Builder setRegenerationCycles(int regenerationCycles) {
            this.regenerationCycles = regenerationCycles;
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

        public Builder setSubmitDispatcher(Executor submitDispatcher) {
            this.submitDispatcher = submitDispatcher;
            return this;
        }

        public Builder setSubmitTimeout(Duration submitTimeout) {
            this.submitTimeout = submitTimeout;
            return this;
        }

        public Builder setSynchronizationCycles(int synchronizationCycles) {
            this.synchronizationCycles = synchronizationCycles;
            return this;
        }

        public Builder setSynchronizeDuration(Duration synchronizeDuration) {
            this.synchronizeDuration = synchronizeDuration;
            return this;
        }

        public Builder setSynchronizeTimeout(Duration synchronizeTimeout) {
            this.synchronizeTimeout = synchronizeTimeout;
            return this;
        }

        public Builder setTransactonTimeout(Duration transactonTimeout) {
            this.submitTimeout = transactonTimeout;
            return this;
        }

        public Builder setTxnPermits(int txnPermits) {
            this.txnPermits = txnPermits;
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
