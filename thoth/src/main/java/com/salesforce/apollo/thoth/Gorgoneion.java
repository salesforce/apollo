/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Duration;
import com.salesfoce.apollo.thoth.proto.AdmissionsGossip;
import com.salesfoce.apollo.thoth.proto.AdmissionsUpdate;
import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Expunge;
import com.salesfoce.apollo.thoth.proto.Registration;
import com.salesfoce.apollo.thoth.proto.SignedAttestation;
import com.salesfoce.apollo.thoth.proto.SignedNonce;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingCommunications.Destination;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.thoth.grpc.admission.Admission;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionClient;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionServer;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionService;
import com.salesforce.apollo.thoth.grpc.admission.gossip.AdmissionReplicationService;
import com.salesforce.apollo.thoth.grpc.admission.gossip.AdmissionsReplication;
import com.salesforce.apollo.thoth.grpc.admission.gossip.AdmissionsReplicationClient;
import com.salesforce.apollo.thoth.grpc.admission.gossip.AdmissionsReplicationServer;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * Apollo attested admission service
 *
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class Gorgoneion {
    private class Admissions implements Admission {
        @Override
        public SignedNonce apply(Registration request, Digest from) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Admittance register(SignedAttestation request, Digest from) {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private class Replication implements AdmissionsReplication {
        @Override
        public void expunge(Expunge expunge, Digest from) {
            // TODO Auto-generated method stub

        }

        @Override
        public AdmissionsUpdate gossip(AdmissionsGossip gossip, Digest from) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void update(AdmissionsUpdate update, Digest from) {
            // TODO Auto-generated method stub

        }
    }

    private static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    private final CommonCommunications<AdmissionService, Admission>                        admissionComms;
    private final Admissions                                                               admissions  = new Admissions();
    private final Clock                                                                    clock;
    private final Context<Member>                                                          context;
    private final DigestAlgorithm                                                          digestAlgo;
    private final SecureRandom                                                             entropy;
    private final Executor                                                                 exec;
    private final ConcurrentNavigableMap<Digest, Expunge>                                  expunged    = new ConcurrentSkipListMap<>();
    private final double                                                                   fpr;
    private final RingCommunications<Member, AdmissionReplicationService>                  gossiper;
    private final KERL                                                                     kerl;
    private final ControlledIdentifierMember                                               member;
    private final ConcurrentNavigableMap<Digest, SignedNonce>                              pending     = new ConcurrentSkipListMap<>();
    private final AdmissionsReplication                                                    replication = new Replication();
    private final CommonCommunications<AdmissionReplicationService, AdmissionsReplication> replicationComms;

    private final AtomicBoolean started = new AtomicBoolean();

    public Gorgoneion(ControlledIdentifierMember member, Context<Member> context, Router admissionsRouter, KERL kerl,
                      Router replicationRouter, Executor executor, Clock clock, SecureRandom entropy,
                      DigestAlgorithm digestAlgo, double fpr) {
        this.clock = clock;
        this.digestAlgo = digestAlgo;
        this.entropy = entropy;
        this.fpr = fpr;
        this.member = member;
        this.context = context;
        this.kerl = kerl;
        this.exec = executor;
        replicationComms = replicationRouter.create(member, context.getId(), replication, "replication",
                                                    r -> new AdmissionsReplicationServer(r,
                                                                                         replicationRouter.getClientIdentityProvider(),
                                                                                         executor, null),
                                                    AdmissionsReplicationClient.getCreate(context.getId(), null),
                                                    AdmissionsReplicationClient.getLocalLoopback(replication, member));
        admissionComms = replicationRouter.create(member, context.getId(), admissions, "admissions",
                                                  r -> new AdmissionServer(r,
                                                                           admissionsRouter.getClientIdentityProvider(),
                                                                           executor, null),
                                                  AdmissionClient.getCreate(context.getId(), null),
                                                  AdmissionClient.getLocalLoopback(admissions, member));
        gossiper = new RingCommunications<>(context, member, replicationComms, executor);
    }

    public void start(Duration frequency, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        gossip(frequency, scheduler);
    }

    private BloomFilter<Digest> getBff(long seed, double p) {
        var biff = new DigestBloomFilter(seed, Math.min(100, pending.size()), p);
        pending.entrySet()
               .forEach(e -> biff.add(e.getKey()
                                       .prefix(JohnHancock.from(e.getValue().getSignature()).toDigest(digestAlgo))));
        return biff;
    }

    private ListenableFuture<AdmissionsUpdate> gossip(AdmissionReplicationService link, Integer ring) {
        return link.gossip(AdmissionsGossip.newBuilder()
                                           .setBff(getBff(Entropy.nextBitsStreamLong(), fpr).toBff())
                                           .build());
    }

    private void gossip(Duration frequency, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        exec.execute(Utils.wrapped(() -> gossiper.execute((link, ring) -> gossip(link, ring),
                                                          (futureSailor, destination) -> gossip(futureSailor,
                                                                                                destination, frequency,
                                                                                                scheduler)),
                                   log));
    }

    private void gossip(Optional<ListenableFuture<AdmissionsUpdate>> futureSailor,
                        Destination<Member, AdmissionReplicationService> destination, Duration frequency,
                        ScheduledExecutorService scheduler) {

    }
}
