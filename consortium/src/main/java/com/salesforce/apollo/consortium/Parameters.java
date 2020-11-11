/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import com.google.common.base.Supplier;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.HashKey;

public class Parameters {
    public static class Builder {
        private Router                                        communications;
        private Function<CertifiedBlock, HashKey>             consensus;
        private Context<Member>                               context;
        private Function<List<Transaction>, List<ByteBuffer>> executor;
        private Duration                                      gossipDuration;
        private Member                                        member;
        private Messenger.Parameters                          msgParameters;
        private ScheduledExecutorService                      scheduler;
        private Supplier<Signature>                           signature;

        public Parameters build() {
            return new Parameters(context, communications, executor, member, msgParameters, scheduler, signature,
                    gossipDuration, consensus);
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

        public Function<List<Transaction>, List<ByteBuffer>> getExecutor() {
            return executor;
        }

        public Duration getGossipDuration() {
            return gossipDuration;
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

        public Parameters.Builder setExecutor(Function<List<Transaction>, List<ByteBuffer>> executor) {
            this.executor = executor;
            return this;
        }

        public Parameters.Builder setGossipDuration(Duration gossipDuration) {
            this.gossipDuration = gossipDuration;
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
    }

    public static Parameters.Builder newBuilder() {
        return new Builder();
    }

    final Router                                                communications;
    final Function<CertifiedBlock, HashKey>                     consensus;
    final Context<Member>                                       context;
    final Duration                                              gossipDuration;
    final Member                                                member;
    final Messenger.Parameters                                  msgParameters;
    final ScheduledExecutorService                              scheduler;
    final Supplier<Signature>                                   signature;
    @SuppressWarnings("unused")
    private final Function<List<Transaction>, List<ByteBuffer>> executor;

    public Parameters(Context<Member> context, Router communications,
            Function<List<Transaction>, List<ByteBuffer>> executor, Member member, Messenger.Parameters msgParameters,
            ScheduledExecutorService scheduler, Supplier<Signature> signature, Duration gossipDuration,
            Function<CertifiedBlock, HashKey> consensus) {
        this.context = context;
        this.communications = communications;
        this.executor = executor;
        this.member = member;
        this.msgParameters = msgParameters;
        this.scheduler = scheduler;
        this.signature = signature;
        this.gossipDuration = gossipDuration;
        this.consensus = consensus;
    }
}
