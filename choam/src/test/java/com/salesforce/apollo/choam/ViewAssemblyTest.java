package com.salesforce.apollo.choam;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.choam.proto.Reassemble;
import com.salesforce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.StaticContext;
import com.salesforce.apollo.cryptography.*;
import com.salesforce.apollo.cryptography.proto.PubKey;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ViewAssemblyTest {

    private static final short                     CARDINALITY    = 4;
    private final        Map<Member, ViewAssembly> assemblies     = new HashMap<>();
    private final        Map<Member, Router>       communications = new HashMap<>();
    private final        List<Ethereal>            controllers    = new ArrayList<>();
    private final        List<ChRbcGossip>         gossipers      = new ArrayList<>();
    private              CountDownLatch            complete;
    private              Context<Member>           context;
    private              Map<Member, VDataSource>  dataSources;
    private              List<Member>              members;
    private              Digest                    nextViewId;

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
                           .map(ControlledIdentifierMember::new)
                           .map(e -> (Member) e)
                           .toList();

        final var prefix = UUID.randomUUID().toString();
        members.forEach(m -> {
            var com = new LocalServer(prefix, m).router(ServerConnectionCache.newBuilder());
            communications.put(m, com);
        });
        context = new StaticContext<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(2), 0.1, members, 3);
        nextViewId = context.getId().prefix(0x666);

        dataSources = members.stream()
                             .map(m -> (SigningMember) m)
                             .collect(Collectors.toMap(m -> m, m -> new VDataSource()));
        complete = new CountDownLatch(context.totalCount());
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
                                              .setGenerateGenesis(true)
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
                                     .setDiadem(DigestAlgorithm.DEFAULT.getLast().toDigeste())
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
                                                                                        Terminal.getLocalLoopback(
                                                                                        (SigningMember) m,
                                                                                        servers.get(m)))));

        Map<Member, Verifier> validators = consensusPairs.entrySet()
                                                         .stream()
                                                         .collect(Collectors.toMap(e -> e.getKey(),
                                                                                   e -> new Verifier.DefaultVerifier(
                                                                                   e.getValue().getPublic())));
        Map<Member, ViewContext> views = new HashMap<>();
        context.allMembers().forEach(m -> {
            SigningMember sm = (SigningMember) m;
            Router router = communications.get(m);
            ViewContext view = new ViewContext(context, params.build(
            RuntimeParameters.newBuilder().setContext(context).setMember(sm).setCommunications(router).build()),
                                               () -> new CHOAM.PendingView(context, DigestAlgorithm.DEFAULT.getLast()),
                                               new Signer.SignerImpl(consensusPairs.get(m).getPrivate(), ULong.MIN),
                                               validators, null);
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
        var builder = Config.newBuilder().setnProc(CARDINALITY);

        for (short i = 0; i < CARDINALITY; i++) {
            final short pid = i;
            final var member = members.get(i);
            ViewAssembly assembly = assemblies.get(member);
            BiConsumer<List<ByteString>, Boolean> blocker = (pb, last) -> {
                assembly.inbound().accept(process(pb, last));
            };
            var controller = new Ethereal(builder.setSigner((Signer) members.get(i)).setPid(pid).build(), 1024 * 1024,
                                          dataSources.get(member), blocker, ep -> {
            }, Integer.toString(i));

            var gossiper = new ChRbcGossip(context, (SigningMember) member, controller.processor(),
                                           communications.get(member), null);
            gossipers.add(gossiper);
            controllers.add(controller);
        }
    }

    private List<Reassemble> process(List<ByteString> preblock, Boolean last) {
        return preblock.stream().map(bs -> {
            try {
                return Reassemble.parseFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }

    private static class VDataSource implements DataSource {
        private final BlockingQueue<Reassemble> outbound = new ArrayBlockingQueue<>(100);

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
