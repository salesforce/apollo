/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.security.Signature;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import com.google.common.base.Supplier;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.HashKey;

public class Parameters {
    public static class Builder {
        private Router                                    communications;
        private Function<CertifiedBlock, HashKey>         consensus;
        private Context<Member>                           context;
        private Duration                                  gossipDuration;
        private int                                       maxBatchByteSize = 4 * 1024;
        private int                                       maxBatchSize     = 10;
        private Member                                    member;
        private Messenger.Parameters                      msgParameters;
        private ScheduledExecutorService                  scheduler;
        private Supplier<Signature>                       signature;
        private Function<EnqueuedTransaction, ByteString> validator;

        public Parameters build() {
            return new Parameters(context, communications, member, msgParameters, scheduler, signature, gossipDuration,
                    consensus, maxBatchSize, maxBatchByteSize, validator);
        }

        public Router getCommunications() {
            return communications;
        }

        public Function<CertifiedBlock, HashKey> getConsensus() {
            return consensus;
        }

        public Context<Member> getContext() {
            return context;
        }

        public Duration getGossipDuration() {
            return gossipDuration;
        }

        public int getMaxBatchByteSize() {
            return maxBatchByteSize;
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

        public ScheduledExecutorService getScheduler() {
            return scheduler;
        }

        public Supplier<Signature> getSignature() {
            return signature;
        }

        public Function<EnqueuedTransaction, ByteString> getValidator() {
            return validator;
        }

        public Parameters.Builder setCommunications(Router communications) {
            this.communications = communications;
            return this;
        }

        public Builder setConsensus(Function<CertifiedBlock, HashKey> consensus) {
            this.consensus = consensus;
            return this;
        }

        @SuppressWarnings("unchecked")
        public Parameters.Builder setContext(Context<? extends Member> context) {
            this.context = (Context<Member>) context;
            return this;
        }

        public Parameters.Builder setGossipDuration(Duration gossipDuration) {
            this.gossipDuration = gossipDuration;
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

        public Parameters.Builder setMember(Member member) {
            this.member = member;
            return this;
        }

        public Parameters.Builder setMsgParameters(Messenger.Parameters msgParameters) {
            this.msgParameters = msgParameters;
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

        public Builder setValidator(Function<EnqueuedTransaction, ByteString> validator) {
            this.validator = validator;
            return this;
        }
    }

    public static Parameters.Builder newBuilder() {
        return new Builder();
    }

    public final Router                                    communications;
    public final Function<CertifiedBlock, HashKey>         consensus;
    public final Context<Member>                           context;
    public final Duration                                  gossipDuration;
    public final int                                       maxBatchByteSize;
    public final int                                       maxBatchSize;
    public final Member                                    member;
    public final Messenger.Parameters                      msgParameters;
    public final ScheduledExecutorService                  scheduler;
    public final Supplier<Signature>                       signature;
    public final Function<EnqueuedTransaction, ByteString> validator;

    public Parameters(Context<Member> context, Router communications, Member member, Messenger.Parameters msgParameters,
            ScheduledExecutorService scheduler, Supplier<Signature> signature, Duration gossipDuration,
            Function<CertifiedBlock, HashKey> consensus, int maxBatchSize, int maxBatchByteSize,
            Function<EnqueuedTransaction, ByteString> validator) {
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
        this.validator = validator;
    }
}
