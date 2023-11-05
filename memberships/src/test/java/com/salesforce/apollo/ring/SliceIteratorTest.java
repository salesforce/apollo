package com.salesforce.apollo.ring;

import com.google.protobuf.Any;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 **/
public class SliceIteratorTest {
    @Test
    public void smokin() throws Exception {
        var serverMember1 = new SigningMemberImpl(Utils.getMember(0));
        var serverMember2 = new SigningMemberImpl(Utils.getMember(1));
        var pinged1 = new AtomicBoolean();
        var pinged2 = new AtomicBoolean();

        var local1 = new TestItService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return serverMember1;
            }

            @Override
            public Any ping(Any request) {
                pinged1.set(true);
                return Any.getDefaultInstance();
            }
        };
        var local2 = new TestItService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return serverMember2;
            }

            @Override
            public Any ping(Any request) {
                pinged2.set(true);
                return Any.getDefaultInstance();
            }
        };
        final var name = UUID.randomUUID().toString();
        Context<Member> context = Context.newBuilder().build();
        context.activate(serverMember1);
        context.activate(serverMember2);

        var serverBuilder = InProcessServerBuilder.forName(name);
        var cacheBuilder = ServerConnectionCache.newBuilder()
                                                .setFactory(to -> InProcessChannelBuilder.forName(name).build());
        var router = new RouterImpl(serverMember1, serverBuilder, cacheBuilder, null);
        RouterImpl.CommonCommunications<TestItService, TestIt> commsA = router.create(serverMember1, context.getId(),
                                                                                      new ServiceImpl(local1, "A"), "A",
                                                                                      ServerImpl::new,
                                                                                      TestItClient::new, local1);

        RouterImpl.CommonCommunications<TestItService, TestIt> commsB = router.create(serverMember2, context.getId(),
                                                                                      new ServiceImpl(local2, "B"), "B",
                                                                                      ServerImpl::new,
                                                                                      TestItClient::new, local2);

        router.start();
        var slice = new SliceIterator<TestItService>("Test Me", serverMember1,
                                                     Arrays.asList(serverMember1, serverMember2), commsA);
        var countdown = new CountDownLatch(1);
        slice.iterate((link, member) -> link.ping(Any.getDefaultInstance()), (result, comms, member) -> true, () -> {
            countdown.countDown();
        }, Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()), Duration.ofMillis(1));
        boolean finished = countdown.await(3, TimeUnit.SECONDS);
        assertTrue(finished, "completed: " + countdown.getCount());
        assertTrue(pinged1.get());
        assertTrue(pinged2.get());
    }
}
