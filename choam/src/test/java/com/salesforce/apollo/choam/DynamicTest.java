package com.salesforce.apollo.choam;

import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.support.ExponentialBackoffPolicy;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.DynamicContext;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 **/
public class DynamicTest {
    private static final int cardinality         = 10;
    private static final int checkpointBlockSize = 10;

    private List<Member>                        members;
    private Map<Member, Router>                 routers;
    private Map<Member, CHOAM>                  choams;
    private Map<Member, DynamicContext<Member>> contexts;

    @BeforeEach
    public void setUp() throws Exception {
        choams = new HashMap<>();
        contexts = new HashMap<>();
        var contextBuilder = DynamicContext.newBuilder().setBias(3);
        SecureRandom entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, cardinality)
                           .mapToObj(_ -> stereotomy.newIdentifier())
                           .map(ControlledIdentifierMember::new)
                           .map(e -> (Member) e)
                           .toList();
        members = IntStream.range(0, cardinality)
                           .mapToObj(_ -> stereotomy.newIdentifier())
                           .map(ControlledIdentifierMember::new)
                           .map(e -> (Member) e)
                           .toList();

        final var prefix = UUID.randomUUID().toString();
        routers = members.stream()
                         .collect(Collectors.toMap(m -> m, m -> new LocalServer(prefix, m).router(
                         ServerConnectionCache.newBuilder().setTarget(cardinality * 2))));

        var template = Parameters.newBuilder()
                                 .setGenerateGenesis(true)
                                 .setBootstrap(Parameters.BootstrapParameters.newBuilder()
                                                                             .setGossipDuration(Duration.ofMillis(20))
                                                                             .build())
                                 .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin())
                                 .setGossipDuration(Duration.ofMillis(10))
                                 .setProducer(Parameters.ProducerParameters.newBuilder()
                                                                           .setGossipDuration(Duration.ofMillis(20))
                                                                           .setBatchInterval(Duration.ofMillis(10))
                                                                           .setMaxBatchByteSize(1024 * 1024)
                                                                           .setMaxBatchCount(10_000)
                                                                           .setEthereal(Config.newBuilder()
                                                                                              .setNumberOfEpochs(3)
                                                                                              .setEpochLength(20))
                                                                           .build())
                                 .setCheckpointBlockDelta(checkpointBlockSize)
                                 .setDrainPolicy(ExponentialBackoffPolicy.newBuilder()
                                                                         .setInitialBackoff(Duration.ofMillis(1))
                                                                         .setMaxBackoff(Duration.ofMillis(1)));

        members.subList(0, 4).forEach(m -> {
            var context = (DynamicContext<Member>) contextBuilder.build();
            contexts.put(m, context);

            choams.put(m, constructCHOAM((SigningMember) m, template.clone().setGenerateGenesis(true), context));
        });
        members.subList(4, members.size()).forEach(m -> {
            var context = (DynamicContext<Member>) contextBuilder.build();
            contexts.put(m, context);
            choams.put(m, constructCHOAM((SigningMember) m, template.clone().setGenerateGenesis(false), context));
        });
    }

    @Test
    public void smokin() throws Exception {
        var bootstrap = members.subList(0, 4);

        bootstrap.forEach(member -> bootstrap.forEach(m -> contexts.get(member).activate(m)));

        bootstrap.parallelStream().forEach(member -> {
            routers.get(member).start();
            choams.get(member).start();
        });
        boolean active = Utils.waitForCondition(10_000, 1_000, () -> bootstrap.stream()
                                                                              .map(m -> choams.get(m))
                                                                              .allMatch(CHOAM::active));
        assertTrue(active, "Bootstrap did not become active, inactive: " + bootstrap.stream()
                                                                                    .map(m -> choams.get(m))
                                                                                    .filter(c -> !c.active())
                                                                                    .map(CHOAM::logState)
                                                                                    .toList());
        System.out.println("**");
        System.out.println("** Bootstrap active: " + bootstrap.stream().map(Member::getId).toList());
        System.out.println("**");

        var next = members.subList(4, 7);
        // Bootstrap group knows about the new members, but not vice versa
        bootstrap.forEach(member -> next.forEach(m -> contexts.get(member).activate(m)));
        // Next group just knows about itself, not the bootstrap group
        next.forEach(member -> next.forEach(m -> contexts.get(member).activate(m)));

        Thread.sleep(2000);

        System.out.println("**");
        System.out.println("** Starting: " + next.stream().map(Member::getId).toList());
        System.out.println("**");
        next.parallelStream().forEach(member -> {
            routers.get(member).start();
            choams.get(member).start();
        });
        Thread.sleep(2000);

        System.out.println("**");
        System.out.println("** Next 3 joining");
        System.out.println("**");
        // now let the next members know about the bootstrap group
        next.forEach(member -> bootstrap.forEach(m -> contexts.get(member).activate(m)));

        active = Utils.waitForCondition(30_000, 1_000,
                                        () -> next.stream().map(m -> choams.get(m)).allMatch(CHOAM::active));
        assertTrue(active, "Next 3 did not become active, inactive: " + next.stream()
                                                                            .map(m -> choams.get(m))
                                                                            .filter(c -> !c.active())
                                                                            .map(CHOAM::logState)
                                                                            .toList());
        System.out.println("**");
        System.out.println("** Next 3 active: " + bootstrap.stream().map(Member::getId).toList());
        System.out.println("**");

        var remaining = members.subList(7, members.size());
        // Bootstrap group knows about the new members, but not vice versa
        bootstrap.forEach(member -> remaining.forEach(m -> contexts.get(member).activate(m)));
        // the next group knows about the new members, but not vice versa
        next.forEach(member -> remaining.forEach(m -> contexts.get(member).activate(m)));

        // the remaining group just knows about itself, not the bootstrap nor the next group
        remaining.forEach(member -> remaining.forEach(m -> contexts.get(member).activate(m)));

        Thread.sleep(2000);

        System.out.println("**");
        System.out.println("** Starting: " + remaining.stream().map(Member::getId).toList());
        System.out.println("**");

        remaining.parallelStream().forEach(member -> {
            routers.get(member).start();
            choams.get(member).start();
        });
        Thread.sleep(2000);

        System.out.println("**");
        System.out.println("** Remaining group joining");
        System.out.println("**");
        // now let the remaining members know about the bootstrap group
        remaining.forEach(member -> bootstrap.forEach(m -> contexts.get(member).activate(m)));
        // and the next group
        remaining.forEach(member -> next.forEach(m -> contexts.get(member).activate(m)));

        active = Utils.waitForCondition(30_000, 1_000,
                                        () -> remaining.stream().map(m -> choams.get(m)).allMatch(CHOAM::active));
        assertTrue(active, "Remaining did not become active, inactive: " + remaining.stream()
                                                                                    .map(m -> choams.get(m))
                                                                                    .filter(c -> !c.active())
                                                                                    .map(CHOAM::logState)
                                                                                    .toList());
        System.out.println("**");
        System.out.println("** Remaining active: " + remaining.stream().map(Member::getId).toList());
        System.out.println("**");
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (choams != null) {
            choams.values().forEach(CHOAM::stop);
            choams = null;
        }
        if (routers != null) {
            routers.values().forEach(e -> e.close(Duration.ofSeconds(1)));
            routers = null;
        }
        members = null;
    }

    private CHOAM constructCHOAM(SigningMember m, Parameters.Builder params, Context<Member> context) {
        final CHOAM.TransactionExecutor processor = (index, hash, t, f, executor) -> {
            if (f != null) {
                f.completeAsync(Object::new, executor);
            }
        };

        params.getProducer().ethereal().setSigner(m);
        return new CHOAM(params.build(Parameters.RuntimeParameters.newBuilder()
                                                                  .setMember(m)
                                                                  .setCommunications(routers.get(m))
                                                                  .setProcessor(processor)
                                                                  .setContext(context)
                                                                  .build()));
    }
}
