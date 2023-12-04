package com.salesforce.apollo.thoth;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.thoth.proto.Thoth_Grpc;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.thoth.grpc.ThothServer;
import io.grpc.Channel;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author hal.hildebrand
 **/
public class ThothServerTest {
    private SecureRandom secureRandom;

    @BeforeEach
    public void before() throws Exception {
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
    }

    @Test
    public void smokin() throws Exception {
        var ks = new MemKeyStore();
        var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        Stereotomy stereotomy = new StereotomyImpl(ks, kerl, secureRandom);
        var member = new ControlledIdentifierMember(stereotomy.newIdentifier());

        var localId = UUID.randomUUID().toString();
        ServerBuilder<?> serverBuilder = InProcessServerBuilder.forName(localId)
                                                               .addService(
                                                               new ThothServer(IdentifierSpecification.newBuilder(),
                                                                               RotationSpecification.newBuilder(),
                                                                               new Thoth(stereotomy)));
        var server = serverBuilder.build();
        server.start();
        try {

            var channel = InProcessChannelBuilder.forName(localId).usePlaintext().build();
            var thoth = new ThothClient(channel);

            ControlledIdentifier<SelfAddressingIdentifier> controller = stereotomy.newIdentifier();

            // delegated inception
            var incp = thoth.inception(controller.getIdentifier());
            assertNotNull(incp);

            var seal = Seal.EventSeal.construct(incp.getIdentifier(), incp.hash(stereotomy.digestAlgorithm()),
                                                incp.getSequenceNumber().longValue());

            var builder = InteractionSpecification.newBuilder().addAllSeals(Collections.singletonList(seal));

            // Commit
            EventCoordinates coords = controller.seal(builder);
            thoth.commit(coords);
            assertNotNull(thoth.identifier());

            // Delegated rotation
            var rot = thoth.rotate();

            assertNotNull(rot);

            seal = Seal.EventSeal.construct(rot.getIdentifier(), rot.hash(stereotomy.digestAlgorithm()),
                                            rot.getSequenceNumber().longValue());

            builder = InteractionSpecification.newBuilder().addAllSeals(Collections.singletonList(seal));

            // Commit
            coords = controller.seal(builder);
            thoth.commit(coords);
        } finally {
            server.shutdown();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    private static class ThothClient {
        private Thoth_Grpc.Thoth_BlockingStub client;

        private ThothClient(Channel channel) {
            this.client = Thoth_Grpc.newBlockingStub(channel);
        }

        public void commit(EventCoordinates coordinates) {
            client.commit(coordinates.toEventCoords());
        }

        public SelfAddressingIdentifier identifier() {
            return (SelfAddressingIdentifier) Identifier.from(client.identifier(Empty.getDefaultInstance()));
        }

        public InceptionEvent inception(SelfAddressingIdentifier identifier) {
            return ProtobufEventFactory.toKeyEvent(client.inception(identifier.toIdent()));
        }

        public RotationEvent rotate() {
            return ProtobufEventFactory.toKeyEvent(client.rotate(Empty.getDefaultInstance()));
        }
    }
}
