package com.salesforce.apollo.leyden.comm.binding;

import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.leyden.proto.BinderGrpc;
import com.salesforce.apollo.leyden.proto.Binding;
import com.salesforce.apollo.leyden.proto.Bound;
import com.salesforce.apollo.leyden.proto.KeyAndToken;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

import java.io.IOException;

/**
 * @author hal.hildebrand
 **/
public class Bind implements BinderClient {
    private final ManagedServerChannel          channel;
    private final BinderMetrics                 metrics;
    private final BinderGrpc.BinderBlockingStub client;

    public Bind(ManagedServerChannel channel, BinderMetrics metrics) {
        this.channel = channel;
        this.metrics = metrics;
        this.client = BinderGrpc.newBlockingStub(channel);
    }

    public static BinderClient getCreate(ManagedServerChannel c, BinderMetrics binderMetrics) {
        return new Bind(c, binderMetrics);
    }

    public static BinderClient getLocalLoopback(BinderService service, SigningMember member) {
        return new BinderClient() {
            @Override
            public void bind(Binding binding) {
                service.bind(binding, member.getId());
            }

            @Override
            public void close() throws IOException {
                // no op
            }

            @Override
            public Bound get(KeyAndToken key) {
                return service.get(key, member.getId());
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public void unbind(KeyAndToken key) {
                service.unbind(key, member.getId());
            }
        };
    }

    @Override
    public void bind(Binding binding) {
        client.bind(binding);
    }

    @Override
    public void close() throws IOException {
        channel.release();
    }

    @Override
    public Bound get(KeyAndToken key) {
        return client.get(key);
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public void unbind(KeyAndToken key) {
        client.unbind(key);
    }
}
