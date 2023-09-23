package com.salesforce.apollo.ring;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author hal.hildebrand
 **/
public class RingIteratorSyncTest {
    public void smokin() throws Exception {
        Duration frequency = Duration.ofMillis(100);
        Context<Member> context = Context.newBuilder().build();
        SigningMember member = (SigningMember) Utils.getMember(0);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        RouterImpl.CommonCommunications<Link, ?> comm = null;
        Executor exec = Executors.newVirtualThreadPerTaskExecutor();
        var sync = new RingIteratorSync<>(frequency, context, member, scheduler, comm, exec);

        sync.iterate(Digest.NONE, (link, round) -> null, (m, result, link) -> false);
    }
}
