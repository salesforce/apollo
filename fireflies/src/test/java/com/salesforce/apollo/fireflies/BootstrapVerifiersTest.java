package com.salesforce.apollo.fireflies;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.proto.Digeste;
import com.salesforce.apollo.fireflies.comm.entrance.Entrance;
import com.salesforce.apollo.fireflies.comm.entrance.EntranceClient;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.IdentAndSeq;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.joou.ULong;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author hal.hildebrand
 **/
public class BootstrapVerifiersTest {

    @Test
    public void smokin() throws Exception {
        int cardinality = 5;
        int majority = 3;

        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        var members = IntStream.range(0, cardinality)
                               .mapToObj(i -> stereotomy.newIdentifier())
                               .map(cpk -> new ControlledIdentifierMember(cpk))
                               .collect(Collectors.toList());
        var member = members.getFirst();

        Digeste testDigest = DigestAlgorithm.DEFAULT.getLast().toDigeste();
        Entrance client = mock(Entrance.class);
        SettableFuture<KeyState_> ks = SettableFuture.create();
        ks.set(KeyState_.newBuilder().setDigest(testDigest).build());
        when(client.getKeyState(any(EventCoords.class))).then(new Answer<>() {
            @Override
            public ListenableFuture<KeyState_> answer(InvocationOnMock invocation) throws Throwable {
                return ks;
            }
        });
        when(client.getKeyState(any(IdentAndSeq.class))).then(new Answer<>() {
            @Override
            public ListenableFuture<KeyState_> answer(InvocationOnMock invocation) throws Throwable {
                return ks;
            }
        });
        when(client.getMember()).then(new Answer<>() {
            @Override
            public Member answer(InvocationOnMock invocation) throws Throwable {
                return member;
            }
        });

        @SuppressWarnings("unchecked")
        RouterImpl.CommonCommunications<Entrance, ?> comm = mock(RouterImpl.CommonCommunications.class);
        when(comm.connect(any())).thenReturn(client);

        RouterImpl.CommonCommunications<EntranceClient, ?> communications = null;
        var verifiers = new Bootstrapper(member, Duration.ofSeconds(1), members, majority, Duration.ofMillis(10), comm);

        var verifier = verifiers.verifierFor(member.getIdentifier().getIdentifier());
        assertFalse(verifier.isEmpty());

        JohnHancock signature = new JohnHancock(SignatureAlgorithm.DEFAULT, new byte[0], ULong.valueOf(0));
        var is = new ByteArrayInputStream(new byte[0]);
        var keyState = verifier.get().verify(signature, is);
        assertNotNull(keyState);
    }
}
