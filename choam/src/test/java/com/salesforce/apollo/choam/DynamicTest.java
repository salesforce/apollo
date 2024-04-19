package com.salesforce.apollo.choam;

import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.DynamicContext;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
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

/**
 * @author hal.hildebrand
 **/
public class DynamicTest {
    private static final int cardinality         = 10;
    private static final int checkpointBlockSize = 10;

    private List<Member>                        members;
    private SecureRandom                        entropy;
    private Map<Member, Router>                 routers;
    private Map<Member, CHOAM>                  choams;
    private Map<Member, DynamicContext<Member>> contexts;

    @BeforeEach
    public void setUp() throws Exception {
        choams = new HashMap<>();
        contexts = new HashMap<>();
        var contextBuilder = DynamicContext.newBuilder().setBias(3);
        entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, cardinality)
                           .mapToObj(i -> stereotomy.newIdentifier())
                           .map(ControlledIdentifierMember::new)
                           .map(e -> (Member) e)
                           .toList();
        members = IntStream.range(0, cardinality)
                           .mapToObj(i -> stereotomy.newIdentifier())
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
                                                                           .build())
                                 .setGenerateGenesis(true)
                                 .setCheckpointBlockDelta(checkpointBlockSize);
        members.forEach(m -> {
            var context = contextBuilder.build();
            contexts.put(m, (DynamicContext<Member>) context);
            choams.put(m, constructCHOAM((SigningMember) m, template.clone(), context));
        });
    }

    @Test
    public void smokin() throws Exception {

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
        var choam = new CHOAM(params.build(Parameters.RuntimeParameters.newBuilder()
                                                                       .setMember(m)
                                                                       .setCommunications(routers.get(m))
                                                                       .setProcessor(processor)
                                                                       .setContext(context)
                                                                       .build()));
        return choam;
    }
}
