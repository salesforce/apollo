package com.salesforce.apollo.leyden;

import com.salesforce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.thoth.proto.Update;
import com.salesforce.apollo.thoth.proto.Updating;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.leyden.comm.*;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 **/
public class LeydenJar {

    private final Context<Member>                                                              context;
    private final RouterImpl.CommonCommunications<ReconciliationClient, ReconciliationService> comms;
    private final double                                                                       fpr;

    public LeydenJar(SigningMember member, Context<Member> context, Router communications, double fpr,
                     ReconciliationMetrics metrics) {
        this.context = context;
        ReconciliationService service = new Reconciled();

        comms = communications.create(member, context.getId(), service, service.getClass().getCanonicalName(),
                                      r -> new ReconciliationServer(r, communications.getClientIdentityProvider(),
                                                                    metrics), c -> Reckoning.getCreate(c, metrics),
                                      Reckoning.getLocalLoopback(service, member));
        this.fpr = fpr;
    }

    private class Reconciled implements ReconciliationService {

        @Override
        public Update reconcile(Intervals request, Digest from) {
            return null;
        }

        @Override
        public void update(Updating request, Digest from) {

        }
    }
}
