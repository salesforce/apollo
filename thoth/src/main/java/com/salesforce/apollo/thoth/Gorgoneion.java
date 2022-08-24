/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.salesfoce.apollo.thoth.proto.AdmissionsGossip;
import com.salesfoce.apollo.thoth.proto.AdmissionsUpdate;
import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Registration;
import com.salesfoce.apollo.thoth.proto.SignedAttestation;
import com.salesfoce.apollo.thoth.proto.SignedNonce;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
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
        public AdmissionsUpdate gossip(AdmissionsGossip gossip, Digest from) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void update(AdmissionsUpdate update, Digest from) {
            // TODO Auto-generated method stub

        }

    }

    private final CommonCommunications<AdmissionService, Admission>                        admissionComms;
    private final Admissions                                                               admissions  = new Admissions();
    private final Context<Member>                                                          context;
    private final RingCommunications<Member, AdmissionReplicationService>                  gossip;
    private final KERL                                                                     kerl;
    private final ControlledIdentifierMember                                               member;
    private final ConcurrentNavigableMap<Digest, SignedNonce>                              pending     = new ConcurrentSkipListMap<>();
    private final AdmissionsReplication                                                    replication = new Replication();
    private final CommonCommunications<AdmissionReplicationService, AdmissionsReplication> replicationComms;
    private final AtomicBoolean                                                            started     = new AtomicBoolean();

    public Gorgoneion(ControlledIdentifierMember member, Context<Member> context, Router admissionsRouter, KERL kerl,
                      Router replicationRouter, Executor executor) {
        this.member = member;
        this.context = context;
        this.kerl = kerl;
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
        gossip = null;
    }
}
