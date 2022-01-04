/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.mvlog;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Any;
import com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.RotationEvent;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.DelegatingEventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.SealingEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.DelegatedInceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.InceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.InteractionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * @author hal.hildebrand
 *
 */
public class MvLog implements KERL {

    private static class ProtobuffDataType extends BasicDataType<Object> {

        @Override
        public int compare(Object a, Object b) {
            return 0;
        }

        @Override
        public Object[] createStorage(int size) {
            return new Object[size];
        }

        @Override
        public int getMemory(Object obj) {
            return ((AbstractMessage) obj).getSerializedSize();
        }

        @Override
        public Object read(ByteBuffer buff) {
            try {
                Any any = Any.parseFrom(BbBackedInputStream.aggregate(buff));
                return wrap(any.unpack(classOf(any)));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read", e);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            Any any = Any.pack((AbstractMessage) obj);
            try {
                any.writeTo(new OutputStream() {

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        buff.put(b, off, len);
                    }

                    @Override
                    public void write(int b) throws IOException {
                        buff.putInt(b);
                    }
                });
            } catch (IOException e) {
                throw new IllegalStateException("Cannot write", e);
            }
        }

        private Class<? extends AbstractMessage> classOf(Any any) {
            if (any.is(com.salesfoce.apollo.stereotomy.event.proto.KeyState.class)) {
                return com.salesfoce.apollo.stereotomy.event.proto.KeyState.class;
            }
            if (any.is(Sig.class)) {
                return Sig.class;
            }
            if (any.is(InceptionEvent.class)) {
                return InceptionEvent.class;
            }
            if (any.is(RotationEvent.class)) {
                return RotationEvent.class;
            }
            if (any.is(InteractionEvent.class)) {
                return InteractionEvent.class;
            }
            throw new IllegalArgumentException("Unknown type: " + any.getTypeUrl());
        }

        private Object wrap(AbstractMessage msg) {
            return switch (msg.getClass().getSimpleName()) {
            case "StoredKeyState": {
                yield new KeyStateImpl((com.salesfoce.apollo.stereotomy.event.proto.KeyState) msg);
            }
            case "Signatures": {
                yield msg;
            }
            case "InceptionEvent": {
                InceptionEvent event = (InceptionEvent) msg;
                yield (event.hasDelegatingPrefix()) ? new DelegatedInceptionEventImpl(event)
                                                    : new InceptionEventImpl(event);
            }
            case "InteractionEvent": {
                yield new InteractionEventImpl((InteractionEvent) msg);
            }
            default:
                throw new IllegalArgumentException("Unexpected message type: " + msg.getClass());
            };
        }

    }

    private static final String EVENTS                  = "EVENTS";
    private static final String EVENTS_BY_HASH          = "EVENTS_BY_HASH";
    private static final String KEY_STATE               = "KEY_STATE";
    private static final String KEY_STATE_BY_IDENTIFIER = "KEY_STATE_BY_IDENTIFIER";
    private static final String LOCATION_TO_HASH        = "LOCATION_TO_HASH";
    private static final String RECEIPTS                = "RECEIPTS";

    /**
     * Ordering by
     * 
     * <pre>
     * <coords.identifier, coords.sequenceNumber, coords.digest>
     * </pre>
     */
    public static String coordinateOrdering(EventCoordinates coords) {
        return qb64(coords.getIdentifier()) + ':' + coords.getSequenceNumber() + ':' + qb64(coords.getDigest());
    }

    public static String receiptDigestSuffix(EventCoordinates event, EventCoordinates signer) {
        return qb64(event.getDigest()) + ':' + qb64(signer.getDigest());
    }

    /**
     * Ordering by
     * 
     * <pre>
     * <event.identifier, signer.identifier, event.sequenceNumber, signer.sequenceNumber, event.digest, signer.digest>
     * </pre>
     */
    public static String receiptOrdering(EventCoordinates event, EventCoordinates signer) {
        return MvLog.receiptPrefix(event, signer) + MvLog.receiptSequence(event, signer)
        + receiptDigestSuffix(event, signer);
    }

    public static String receiptPrefix(EventCoordinates event, EventCoordinates signer) {
        return MvLog.receiptPrefix(event.getIdentifier(), signer.getIdentifier());
    }

    public static String receiptPrefix(Identifier forIdentifier, Identifier forIdentifier2) {
        return qb64(forIdentifier) + ':' + qb64(forIdentifier2) + '.';
    }

    public static String receiptSequence(EventCoordinates event, EventCoordinates signer) {
        return Long.toString(event.getSequenceNumber()) + ':' + signer.getSequenceNumber() + '.';
    }

    private final DigestAlgorithm digestAlgorithm;
    // Order by <stateOrdering>
    private final MVMap<String, KeyEvent> events;
    private final MVMap<String, String>   eventsByHash;

    // Order by <stateOrdering>
    private final MVMap<String, KeyState> keyState;

    // Order by <identifier>
    private final MVMap<String, String> keyStateByIdentifier;

    private final MVMap<String, String> locationToHash;

    private final KeyEventProcessor processor = new KeyEventProcessor(this);

    // Order by <receiptOrdering>
    private final MVMap<String, AttachmentEvent.Attachment> receipts;

    public MvLog(DigestAlgorithm digestAlgorithm, MVStore store) {
        this.digestAlgorithm = digestAlgorithm;
        ProtobuffDataType serializer = new ProtobuffDataType();

        events = store.openMap(EVENTS, new MVMap.Builder<String, KeyEvent>().valueType(serializer));
        keyState = store.openMap(KEY_STATE, new MVMap.Builder<String, KeyState>().valueType(serializer));
        keyStateByIdentifier = store.openMap(KEY_STATE_BY_IDENTIFIER);
        receipts = store.openMap(RECEIPTS,
                                 new MVMap.Builder<String, AttachmentEvent.Attachment>().valueType(serializer));
        eventsByHash = store.openMap(EVENTS_BY_HASH);
        locationToHash = store.openMap(LOCATION_TO_HASH);
    }

    @Override
    public void append(AttachmentEvent event) {
        appendAttachments(event.coordinates(), event.attachments());
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        final var newState = processor.process(event);
        append(event, newState);
        var f = new CompletableFuture<KeyState>();
        f.complete(newState);
        return f;
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    @Override
    public Optional<SealingEvent> getKeyEvent(DelegatingEventCoordinates coordinates) {
        KeyEvent keyEvent = events.get(MvLog.coordinateOrdering(new EventCoordinates(coordinates.getIdentifier(),
                                                                                     coordinates.getSequenceNumber(),
                                                                                     coordinates.getPreviousEvent()
                                                                                                .getDigest(),
                                                                                     coordinates.getIlk())));
        return (keyEvent instanceof SealingEvent) ? Optional.of((SealingEvent) keyEvent) : Optional.empty();
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(Digest digest) {
        String coordinates = eventsByHash.get(qb64(digest));
        return coordinates == null ? Optional.empty() : Optional.of(events.get(coordinates));
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return Optional.ofNullable(events.get(MvLog.coordinateOrdering(coordinates)));
    }

    @Override
    public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
        return Optional.ofNullable(keyState.get(MvLog.coordinateOrdering(coordinates)));
    }

    @Override
    public Optional<KeyState> getKeyState(Identifier identifier) {
        String stateHash = keyStateByIdentifier.get(qb64(identifier));

        return stateHash == null ? Optional.empty() : Optional.ofNullable(keyState.get(stateHash));
    }

    private void append(KeyEvent event, KeyState newState) {
        String coordinates = MvLog.coordinateOrdering(event.getCoordinates());
        events.put(coordinates, event);
        String hashstring = qb64(newState.getDigest());
        eventsByHash.put(hashstring, coordinates);
        locationToHash.put(coordinates, hashstring);
        keyState.put(coordinates, newState);
        keyStateByIdentifier.put(qb64(event.getIdentifier()), coordinates);
    }

    private void appendAttachments(EventCoordinates coordinates, AttachmentEvent.Attachment attachment) {
        var key = MvLog.coordinateOrdering(coordinates);
        var previous = receipts.get(key);
        receipts.put(key, combine(attachment, previous));
    }

    private Attachment combine(Attachment attachment, Attachment previous) {
        if (previous == null) {
            return attachment;
        }
        List<Seal> seals = new ArrayList<>(previous.seals());
        seals.addAll(attachment.seals());
        Map<Integer, JohnHancock> endorsements = new HashMap<>(previous.endorsements());
        endorsements.putAll(attachment.endorsements());

        return new AttachmentEvent.Attachment() {

            @Override
            public Map<Integer, JohnHancock> endorsements() {
                return endorsements;
            }

            @Override
            public List<Seal> seals() {
                return seals;
            }
        };
    }
}
