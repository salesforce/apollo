package com.salesforce.apollo.ring;

import com.google.protobuf.Any;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.context.DynamicContext;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.joou.ULong;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 **/
public class RingCommunicationsTest {
    @Test
    public void smokin() throws Exception {
        var serverMember1 = new SigningMemberImpl(Utils.getMember(1), ULong.MIN);
        var serverMember2 = new SigningMemberImpl(Utils.getMember(2), ULong.MIN);
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
        DynamicContext<Member> context = DynamicContext.newBuilder().build();
        context.activate(serverMember1);
        context.activate(serverMember2);

        var serverBuilder = InProcessServerBuilder.forName(name);
        var cacheBuilder = ServerConnectionCache.newBuilder()
                                                .setFactory(to -> InProcessChannelBuilder.forName(name).build());
        var router = new RouterImpl(serverMember1, serverBuilder, cacheBuilder, null);
        try {
            var commsA = router.create(serverMember1, context.getId(), new ServiceImpl(local1, "A"), "A",
                                       ServerImpl::new, TestItClient::new, local1);

            router.create(serverMember2, context.getId(), new ServiceImpl(local2, "B"), "B", ServerImpl::new,
                          TestItClient::new, local2);

            router.start();
            var sync = new RingCommunications<Member, TestItService>(context, serverMember1, commsA);
            sync.noDuplicates();
            var countdown = new CountDownLatch(1);
            for (var i = 0; i < context.getRingCount(); i++) {
                sync.execute((link, round) -> link.ping(Any.getDefaultInstance()),
                             (result, destination) -> countdown.countDown());
            }
            assertTrue(countdown.await(1, TimeUnit.SECONDS), "Completed: " + countdown.getCount());
            assertFalse(pinged1.get());
            assertTrue(pinged2.get());
        } finally {
            router.close(Duration.ofSeconds(5));
        }
    }
}
