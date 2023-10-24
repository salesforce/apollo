package com.salesforce.apollo.choam;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Reassemble;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.crypto.*;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ViewAssemblyTest {

    private static short                           CARDINALITY    = 4;
    private        Map<Member, ViewAssembly>       assemblies     = new HashMap<>();
    private        Map<SigningMember, Router>      communications = new HashMap<>();
    private        CountDownLatch                  complete;
    private        Context<Member>                 context;
    private        List<Ethereal>                  controllers    = new ArrayList<>();
    private        Map<SigningMember, VDataSource> dataSources;
    private        List<ChRbcGossip>               gossipers      = new ArrayList<>();
    private        List<SigningMember>             members;
    private        Digest                          nextViewId;

    @AfterEach
    public void after() {
        controllers.forEach(e -> e.stop());
        gossipers.forEach(e -> e.stop());
        communications.values().forEach(e -> e.close(Duration.ofSeconds(1)));
    }

    @BeforeEach
    public void before() throws Exception {

        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, CARDINALITY)
                           .mapToObj(i -> stereotomy.newIdentifier())
                           .map(cpk -> new ControlledIdentifierMember(cpk))
                           .map(e -> (SigningMember) e)
                           .toList();

        final var prefix = UUID.randomUUID().toString();
        members.forEach(m -> {
            var com = new LocalServer(prefix, m, Executors.newSingleThreadExecutor()).router(
            ServerConnectionCache.newBuilder(), Executors.newFixedThreadPool(2));
            communications.put(m, com);
        });
        context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(2), members.size(), 0.1, 3);
        for (Member m : members) {
            context.activate(m);
        }
        nextViewId = context.getId().prefix(0x666);

        dataSources = members.stream().collect(Collectors.toMap(m -> m, m -> new VDataSource()));
        complete = new CountDownLatch(context.activeCount());
        buildAssemblies();
        initEthereals();
    }

    @Test
    public void testIt() throws Exception {

        final var gossipPeriod = Duration.ofMillis(5);

        assemblies.values().forEach(assembly -> assembly.assembled());
        controllers.forEach(e -> e.start());
        communications.values().forEach(e -> e.start());
        gossipers.forEach(e -> e.start(gossipPeriod));
        assertTrue(complete.await(60, TimeUnit.SECONDS), "Failed to reconfigure");
    }

    private void buildAssemblies() {
        Parameters.Builder params = Parameters.newBuilder()
                                              .setProducer(ProducerParameters.newBuilder()
                                                                             .setGossipDuration(Duration.ofMillis(10))
                                                                             .build())
                                              .setGossipDuration(Duration.ofMillis(10));
        Map<Member, Concierge> servers = members.stream().collect(Collectors.toMap(m -> m, m -> mock(Concierge.class)));
        Map<Member, KeyPair> consensusPairs = new HashMap<>();
        servers.forEach((m, s) -> {
            KeyPair keyPair = params.getViewSigAlgorithm().generateKeyPair();
            consensusPairs.put(m, keyPair);
            final PubKey consensus = QualifiedBase64.bs(keyPair.getPublic());
            when(s.join(any(Digest.class), any(Digest.class))).then(new Answer<ViewMember>() {
                @Override
                public ViewMember answer(InvocationOnMock invocation) throws Throwable {
                    return ViewMember.newBuilder()
                                     .setId(m.getId().toDigeste())
                                     .setConsensusKey(consensus)
                                     .setSignature(((Signer) m).sign(consensus.toByteString()).toSig())
                                     .build();
                }
            });
        });
        var comms = members.stream()
                           .collect(Collectors.toMap(m -> m, m -> communications.get(m)
                                                                                .create(m, context.getId(),
                                                                                        servers.get(m), servers.get(m)
                                                                                                               .getClass()
                                                                                                               .getCanonicalName(),
                                                                                        r -> {
                                                                                            Router router = communications.get(
                                                                                            m);
                                                                                            return new TerminalServer(
                                                                                            router.getClientIdentityProvider(),
                                                                                            null, r);
                                                                                        },
                                                                                        TerminalClient.getCreate(null),
                                                                                        Terminal.getLocalLoopback(m,
                                                                                                                  servers.get(
                                                                                                                  m)))));

        Map<Member, Verifier> validators = consensusPairs.entrySet()
                                                         .stream()
                                                         .collect(Collectors.toMap(e -> e.getKey(),
                                                                                   e -> new Verifier.DefaultVerifier(
                                                                                   e.getValue().getPublic())));
        Map<Member, ViewContext> views = new HashMap<>();
        context.active().forEach(m -> {
            SigningMember sm = (SigningMember) m;
            Router router = communications.get(m);
            ViewContext view = new ViewContext(context, params.build(RuntimeParameters.newBuilder()
                                                                                      .setExec(
                                                                                      Executors.newFixedThreadPool(2))
                                                                                      .setContext(context)
                                                                                      .setMember(sm)
                                                                                      .setCommunications(router)
                                                                                      .build()),
                                               new Signer.SignerImpl(consensusPairs.get(m).getPrivate()), validators,
                                               null);
            views.put(m, view);
            var ds = dataSources.get(m);
            var com = comms.get(m);
            assemblies.put(m, new ViewAssembly(nextViewId, view, r -> ds.publish(r), com) {
                @Override
                public void complete() {
                    super.complete();
                    complete.countDown();
                }
            });
        });
    }

    private void initEthereals() {
        var builder = Config.newBuilder()
                            .setnProc(CARDINALITY)
                            .setVerifiers(members.toArray(new Verifier[members.size()]));

        for (short i = 0; i < CARDINALITY; i++) {
            final short pid = i;
            final var member = members.get(i);
            ViewAssembly assembly = assemblies.get(member);
            BiConsumer<PreBlock, Boolean> blocker = (pb, last) -> {
                assembly.inbound().accept(process(pb, last));
            };
            var controller = new Ethereal(builder.setSigner(members.get(i)).setPid(pid).build(), 1024 * 1024,
                                          dataSources.get(member), blocker, ep -> {
            }, Ethereal.consumer(Integer.toString(i)));

            var gossiper = new ChRbcGossip(context, member, controller.processor(), communications.get(member),
                                           Executors.newFixedThreadPool(2), null);
            gossipers.add(gossiper);
            controllers.add(controller);
        }
    }

    private List<Reassemble> process(PreBlock preblock, Boolean last) {
        return preblock.data().stream().map(bs -> {
            try {
                return Reassemble.parseFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    private static class VDataSource implements DataSource {
        private BlockingQueue<Reassemble> outbound = new ArrayBlockingQueue<>(100);

        @Override
        public ByteString getData() {
            Reassemble.Builder result;
            try {
                Reassemble poll = outbound.poll(1, TimeUnit.SECONDS);
                if (poll != null) {
                    result = Reassemble.newBuilder(poll);
                } else {
                    result = Reassemble.newBuilder();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ByteString.EMPTY;
            }
            while (outbound.peek() != null) {
                var current = outbound.poll();
                result.addAllMembers(current.getMembersList()).addAllValidations(current.getValidationsList());
            }
            return result.build().toByteString();
        }

        public void publish(Reassemble r) {
            outbound.add(r);
        }

    }
}
