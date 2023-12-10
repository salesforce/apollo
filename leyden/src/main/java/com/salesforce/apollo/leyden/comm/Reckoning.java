package com.salesforce.apollo.leyden.comm;

import com.salesforce.apollo.leyden.proto.Intervals;
import com.salesforce.apollo.leyden.proto.ReconciliationGrpc;
import com.salesforce.apollo.leyden.proto.Update;
import com.salesforce.apollo.leyden.proto.Updating;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

import java.io.IOException;

/**
 * @author hal.hildebrand
 **/
public class Reckoning implements ReconciliationClient {
    private final ManagedServerChannel                          channel;
    private final ReconciliationGrpc.ReconciliationBlockingStub client;
    private final Member                                        member;

    public Reckoning(ManagedServerChannel channel, Member member, ReconciliationMetrics metrics) {
        this.channel = channel;
        this.client = ReconciliationGrpc.newBlockingStub(channel);
        this.member = member;
    }

    public static ReconciliationClient getCreate(ManagedServerChannel channel, ReconciliationMetrics metrics) {
        return null;
    }

    public static ReconciliationClient getLocalLoopback(ReconciliationService service, SigningMember member) {
        return new ReconciliationClient() {
            @Override
            public void close() throws IOException {

            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public Update reconcile(Intervals intervals) {
                return Update.getDefaultInstance();
            }

            @Override
            public void update(Updating updating) {
                // noop
            }
        };
    }

    @Override
    public void close() throws IOException {
        channel.shutdown();
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public Update reconcile(Intervals intervals) {
        return client.reconcile(intervals);
    }

    @Override
    public void update(Updating updating) {
        client.update(updating);
    }
}
