package com.salesforce.apollo.leyden;

import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.leyden.comm.binding.*;
import com.salesforce.apollo.leyden.comm.reconcile.*;
import com.salesforce.apollo.leyden.proto.Binding;
import com.salesforce.apollo.leyden.proto.Key_;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.thoth.proto.Update;
import com.salesforce.apollo.thoth.proto.Updating;

/**
 * @author hal.hildebrand
 **/
public class LeydenJar {

    private final Context<Member>                                                              context;
    private final RouterImpl.CommonCommunications<ReconciliationClient, ReconciliationService> reconComms;
    private final RouterImpl.CommonCommunications<BinderClient, BinderService>                 binderComms;
    private final double                                                                       fpr;
    private final SigningMember                                                                member;

    public LeydenJar(SigningMember member, Context<Member> context, Router communications, double fpr,
                     ReconciliationMetrics metrics, BinderMetrics binderMetrics) {
        this.context = context;
        this.member = member;
        var recon = new Reconciled();
        reconComms = communications.create(member, context.getId(), recon,
                                           ReconciliationService.class.getCanonicalName(),
                                           r -> new ReconciliationServer(r, communications.getClientIdentityProvider(),
                                                                         metrics), c -> Reckoning.getCreate(c, metrics),
                                           Reckoning.getLocalLoopback(recon, member));

        var borders = new Borders();
        binderComms = communications.create(member, context.getId(), borders, BinderService.class.getCanonicalName(),
                                            r -> new BinderServer(r, communications.getClientIdentityProvider(),
                                                                  binderMetrics), c -> Bind.getCreate(c, binderMetrics),
                                            Bind.getLocalLoopback(borders, member));
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

    private class Borders implements BinderService {

        @Override
        public void bind(Binding request, Digest from) {

        }

        @Override
        public void unbind(Key_ request, Digest from) {

        }
    }
}
