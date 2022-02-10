/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.h2.mvstore.MVStore;
import org.joou.ULong;

import com.salesfoce.apollo.choam.proto.FoundationSeal;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.support.CheckpointState;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.utils.ExponentialBackoff;

import io.grpc.Status;

/**
 * @author hal.hildebrand
 *
 */
public record Parameters(RuntimeParameters runtime, ReliableBroadcaster.Parameters.Builder combine,
                         Duration gossipDuration, int maxCheckpointSegments, Duration submitTimeout,
                         Digest genesisViewId, int checkpointBlockSize, DigestAlgorithm digestAlgorithm,
                         SignatureAlgorithm viewSigAlgorithm, int synchronizationCycles, Duration synchronizeDuration,
                         int regenerationCycles, Duration synchronizeTimeout, BootstrapParameters bootstrap,
                         ProducerParameters producer, int txnPermits, ExponentialBackoff.Builder<Status> clientBackoff,
                         MvStoreBuilder mvBuilder) {

    public int toleranceLevel() {
        final double n = runtime.context.getRingCount();
        return Dag.minimalQuorum((short) n, 3);
    }

    public static class MvStoreBuilder {
        private int     autoCommitBufferSize = -1;
        private int     autoCompactFillRate  = -1;
        private int     cachConcurrency      = -1;
        private int     cachSize             = -1;
        private boolean compress             = false;
        private boolean compressHigh         = false;
        private File    fileName             = null;
        private int     keysPerPage          = -1;
        private int     pageSplitSize        = -1;
        private boolean readOnly             = false;
        private boolean recoveryMode         = false;

        public MVStore build() {
            return build(null);
        }

        public MVStore build(char[] encryptionKey) {
            var builder = new MVStore.Builder();
            if (autoCommitBufferSize > 0) {
                builder.autoCommitBufferSize(autoCommitBufferSize);
            }
            if (autoCompactFillRate > 0) {
                builder.autoCompactFillRate(autoCompactFillRate);
            }
            if (fileName != null) {
                builder.fileName(fileName.getAbsolutePath());
            }
            if (encryptionKey != null) {
                builder.encryptionKey(encryptionKey);
            }
            if (readOnly) {
                builder.readOnly();
            }
            if (keysPerPage > 0) {
                builder.keysPerPage(keysPerPage);
            }
            if (recoveryMode) {
                builder.recoveryMode();
            }
            if (cachSize > 0) {
                builder.cacheSize(cachSize);
            }
            if (cachConcurrency > 0) {
                builder.cacheConcurrency(cachConcurrency);
            }
            if (compress) {
                builder.compress();
            }
            if (compressHigh) {
                builder.compressHigh();
            }
            if (pageSplitSize > 0) {
                builder.pageSplitSize(pageSplitSize);
            }
            return builder.open();
        }
    }

    public record RuntimeParameters(Context<Member> context, Router communications, SigningMember member,
                                    ScheduledExecutorService scheduler,
                                    Function<Map<Member, Join>, List<Transaction>> genesisData,
                                    TransactionExecutor processor, BiConsumer<ULong, CheckpointState> restorer,
                                    Function<ULong, File> checkpointer, ChoamMetrics metrics, Executor exec,
                                    Supplier<KERL> kerl, FoundationSeal foundation) {
        public static class Builder {
            private final static Function<ULong, File>             NULL_CHECKPOINTER = h -> {
                                                                                         File cp;
                                                                                         try {
                                                                                             cp = File.createTempFile("cp-"
                                                                                             + h, ".chk");
                                                                                             cp.deleteOnExit();
                                                                                             try (
                                                                                             var os = new FileOutputStream(cp)) {
                                                                                                 os.write("Give me food or give me slack or kill me".getBytes());
                                                                                             }
                                                                                         } catch (IOException e) {
                                                                                             throw new IllegalStateException(e);
                                                                                         }
                                                                                         return cp;
                                                                                     };
            private Function<ULong, File>                          checkpointer      = NULL_CHECKPOINTER;
            private Router                                         communications;
            private Context<Member>                                context;
            private Executor                                       exec              = ForkJoinPool.commonPool();
            private FoundationSeal                                 foundation        = FoundationSeal.getDefaultInstance();
            private Function<Map<Member, Join>, List<Transaction>> genesisData       = view -> new ArrayList<>();
            private Supplier<KERL>                                 kerl              = () -> KERL.getDefaultInstance();
            private SigningMember                                  member;
            private ChoamMetrics                                   metrics;
            private TransactionExecutor                            processor         = (i, h, t, f) -> {
                                                                                     };
            private BiConsumer<ULong, CheckpointState>             restorer          = (height, checkpointState) -> {
                                                                                     };
            private ScheduledExecutorService                       scheduler;

            public RuntimeParameters build() {
                return new RuntimeParameters(context, communications, member, scheduler, genesisData, processor,
                                             restorer, checkpointer, metrics, exec, kerl, foundation);
            }

            public Function<ULong, File> getCheckpointer() {
                return checkpointer;
            }

            public Router getCommunications() {
                return communications;
            }

            public Context<Member> getContext() {
                return context;
            }

            public Executor getExec() {
                return exec;
            }

            public FoundationSeal getFoundation() {
                return foundation;
            }

            public Function<Map<Member, Join>, List<Transaction>> getGenesisData() {
                return genesisData;
            }

            public Supplier<KERL> getKerl() {
                return kerl;
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

            public BiConsumer<ULong, CheckpointState> getRestorer() {
                return restorer;
            }

            public ScheduledExecutorService getScheduler() {
                return scheduler;
            }

            public Builder setCheckpointer(Function<ULong, File> checkpointer) {
                this.checkpointer = checkpointer;
                return this;
            }

            public Builder setCommunications(Router communications) {
                this.communications = communications;
                return this;
            }

            @SuppressWarnings("unchecked")
            public Builder setContext(Context<? extends Member> context) {
                this.context = (Context<Member>) context;
                return this;
            }

            public Builder setExec(Executor exec) {
                this.exec = exec;
                return this;
            }

            public Builder setFoundation(FoundationSeal foundation) {
                this.foundation = foundation;
                return this;
            }

            public Builder setGenesisData(Function<Map<Member, Join>, List<Transaction>> genesisData) {
                this.genesisData = genesisData;
                return this;
            }

            public Builder setKerl(Supplier<KERL> kerl) {
                this.kerl = kerl;
                return this;
            }

            public Builder setMember(SigningMember member) {
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

            public Builder setRestorer(BiConsumer<ULong, CheckpointState> biConsumer) {
                this.restorer = biConsumer;
                return this;
            }

            public Builder setScheduler(ScheduledExecutorService scheduler) {
                this.scheduler = scheduler;
                return this;
            }
        }

        public static Builder newBuilder() {
            return new Builder();
        }
    }

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

    public record ProducerParameters(Config.Builder ethereal, Duration gossipDuration, int maxBatchByteSize,
                                     Duration batchInterval, int maxBatchCount, Duration maxGossipDelay) {

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private Duration       batchInterval    = Duration.ofMillis(100);
            private Config.Builder ethereal         = Config.deterministic();
            private Duration       gossipDuration   = Duration.ofSeconds(1);
            private int            maxBatchByteSize = 256 * 1024;
            private int            maxBatchCount    = 1_000;
            private Duration       maxGossipDelay   = Duration.ofSeconds(10);

            public ProducerParameters build() {
                return new ProducerParameters(ethereal, gossipDuration, maxBatchByteSize, batchInterval, maxBatchCount,
                                              maxGossipDelay);
            }

            public Duration getBatchInterval() {
                return batchInterval;
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

            public int getMaxBatchCount() {
                return maxBatchCount;
            }

            public Duration getMaxGossipDelay() {
                return maxGossipDelay;
            }

            public Builder setBatchInterval(Duration batchInterval) {
                this.batchInterval = batchInterval;
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

            public Builder setMaxBatchCount(int maxBatchCount) {
                this.maxBatchCount = maxBatchCount;
                return this;
            }

            public Builder setMaxGossipDelay(Duration maxGossipDelay) {
                this.maxGossipDelay = maxGossipDelay;
                return this;
            }
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements Cloneable {

        private BootstrapParameters                    bootstrap             = BootstrapParameters.newBuilder().build();
        private int                                    checkpointBlockSize   = 8192;
        private ExponentialBackoff.Builder<Status>     clientBackoff         = ExponentialBackoff.<Status>newBuilder()
                                                                                                 .retryIf(s -> s.isOk());
        private ReliableBroadcaster.Parameters.Builder combineParams         = ReliableBroadcaster.Parameters.newBuilder();
        private DigestAlgorithm                        digestAlgorithm       = DigestAlgorithm.DEFAULT;
        private Digest                                 genesisViewId;
        private Duration                               gossipDuration        = Duration.ofSeconds(1);
        private int                                    maxCheckpointSegments = 200;
        private MvStoreBuilder                         mvBuilder             = new MvStoreBuilder();
        private ProducerParameters                     producer              = ProducerParameters.newBuilder().build();
        private int                                    regenerationCycles    = 20;
        private Duration                               submitTimeout         = Duration.ofSeconds(30);
        private int                                    synchronizationCycles = 10;
        private Duration                               synchronizeDuration   = Duration.ofMillis(500);
        private Duration                               synchronizeTimeout    = Duration.ofSeconds(30);
        private int                                    txnPermits            = 3_000;
        private SignatureAlgorithm                     viewSigAlgorithm      = SignatureAlgorithm.DEFAULT;

        public Parameters build(RuntimeParameters runtime) {
            return new Parameters(runtime, combineParams, gossipDuration, maxCheckpointSegments, submitTimeout,
                                  genesisViewId, checkpointBlockSize, digestAlgorithm, viewSigAlgorithm,
                                  synchronizationCycles, synchronizeDuration, regenerationCycles, synchronizeTimeout,
                                  bootstrap, producer, txnPermits, clientBackoff, mvBuilder);
        }

        public Builder clone() {
            try {
                return (Builder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException("well, that was unexpected");
            }
        }

        public BootstrapParameters getBootstrap() {
            return bootstrap;
        }

        public int getCheckpointBlockSize() {
            return checkpointBlockSize;
        }

        public ExponentialBackoff.Builder<Status> getClientBackoff() {
            return clientBackoff;
        }

        public ReliableBroadcaster.Parameters.Builder getCombineParams() {
            return combineParams;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
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

        public ProducerParameters getProducer() {
            return producer;
        }

        public int getRegenerationCycles() {
            return regenerationCycles;
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

        public Builder setClientBackoff(ExponentialBackoff.Builder<Status> clientBackoff) {
            this.clientBackoff = clientBackoff;
            return this;
        }

        public Builder setCombineParams(ReliableBroadcaster.Parameters.Builder combineParams) {
            this.combineParams = combineParams;
            return this;
        }

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
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

        public Builder setProducer(ProducerParameters producer) {
            this.producer = producer;
            return this;
        }

        public Builder setRegenerationCycles(int regenerationCycles) {
            this.regenerationCycles = regenerationCycles;
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

        protected MvStoreBuilder getMvBuilder() {
            return mvBuilder;
        }

        protected Builder setMvBuilder(MvStoreBuilder mvBuilder) {
            this.mvBuilder = mvBuilder;
            return this;
        }
    }

    public SigningMember member() {
        return runtime.member;
    }

    public Context<Member> context() {
        return runtime.context;
    }

    public Router communications() {
        return runtime.communications;
    }

    public ChoamMetrics metrics() {
        return runtime.metrics;
    }

    public ScheduledExecutorService scheduler() {
        return runtime.scheduler;
    }

    public Function<ULong, File> checkpointer() {
        return runtime.checkpointer;
    }

    public Function<Map<Member, Join>, List<Transaction>> genesisData() {
        return runtime.genesisData;
    }

    public TransactionExecutor processor() {
        return runtime.processor;
    }

    public BiConsumer<ULong, CheckpointState> restorer() {
        return runtime.restorer;
    }

    public Executor exec() {
        return runtime.exec;
    }

    public Supplier<KERL> kerl() {
        return runtime.kerl;
    }

}
