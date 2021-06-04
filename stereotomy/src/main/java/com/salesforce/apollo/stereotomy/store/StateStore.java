/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.store;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.Identifier.coordinateOrdering;
import static com.salesforce.apollo.stereotomy.identifier.Identifier.receiptOrdering;
import static com.salesforce.apollo.stereotomy.identifier.Identifier.receiptPrefix;
import static com.salesforce.apollo.stereotomy.identifier.Identifier.signatures;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Any;
import com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.RotationEvent;
import com.salesfoce.apollo.stereotomy.event.proto.Signatures;
import com.salesfoce.apollo.stereotomy.event.proto.StoredKeyState;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.DelegatedInceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.DelegatedRotationEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.InceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.InteractionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.RotationEventImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * @author hal.hildebrand
 *
 */
public class StateStore {

    private static class ProtobuffDataType implements DataType {

        @Override
        public int compare(Object a, Object b) {
            return 0;
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
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
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

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        private Class<? extends AbstractMessage> classOf(Any any) {
            if (any.is(StoredKeyState.class)) {
                return StoredKeyState.class;
            }
            if (any.is(Signatures.class)) {
                return Signatures.class;
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
                yield new KeyStateImpl((StoredKeyState) msg);
            }
            case "Signatures": {
                yield msg;
            }
            case "InceptionEvent": {
                InceptionEvent event = (InceptionEvent) msg;
                yield (event.hasDelegatingEvent()) ? new DelegatedInceptionEventImpl(event)
                        : new InceptionEventImpl(event);
            }
            case "RotationEvent": {
                RotationEvent event = (RotationEvent) msg;
                yield (event.hasDelegatingEvent()) ? new DelegatedRotationEventImpl(event)
                        : new RotationEventImpl(event);
            }
            case "InteractionEvent": {
                yield new InteractionEventImpl((InteractionEvent) msg);
            }
            default:
                throw new IllegalArgumentException("Unexpected message type: " + msg.getClass());
            };
        }

    }

    private static final String AUTHENTICATIONS         = "AUTHENTICATIONS";
    private static final String ENDORSEMENTS            = "ENDORSEMENTS";
    private static final String EVENTS                  = "EVENTS";
    private static final String EVENTS_BY_HASH          = "EVENTS";
    private static final String KEY_STATE               = "KEY_STATE";
    private static final String KEY_STATE_BY_IDENTIFIER = "KEY_STATE_BY_IDENTIFIER";
    private static final String LAST_RECEIPT            = "LAST_RECEIPT";
    private static final String RECEIPTS                = "RECEIPTS";

    // Order by <stateOrdering>
    private final MVMap<String, Signatures> authentications;
    // Order by <stateOrdering>
    private final MVMap<String, Signatures> endorsements;
    // Order by <stateOrdering>
    private final MVMap<String, KeyEvent> events;
    private final MVMap<String, String>   eventsByHash;
    // Order by <stateOrdering>
    private final MVMap<String, KeyState> keyState;
    // Order by <identifier>
    private final MVMap<String, String> keyStateByIdentifier;
    // Order by <stateOrdering>
    private final MVMap<String, Long> lastReceipt;
    // Order by <receiptOrdering>
    private final MVMap<String, Signatures> receipts;

    public StateStore(MVStore store) {
        ProtobuffDataType serializer = new ProtobuffDataType();

        authentications = store.openMap(AUTHENTICATIONS, new MVMap.Builder<String, Signatures>().valueType(serializer));
        lastReceipt = store.openMap(LAST_RECEIPT);
        endorsements = store.openMap(ENDORSEMENTS, new MVMap.Builder<String, Signatures>().valueType(serializer));
        events = store.openMap(EVENTS, new MVMap.Builder<String, KeyEvent>().valueType(serializer));
        keyState = store.openMap(KEY_STATE, new MVMap.Builder<String, KeyState>().valueType(serializer));
        keyStateByIdentifier = store.openMap(KEY_STATE_BY_IDENTIFIER);
        receipts = store.openMap(RECEIPTS, new MVMap.Builder<String, Signatures>().valueType(serializer));
        eventsByHash = store.openMap(EVENTS_BY_HASH);
    }

    public void append(KeyEvent event, KeyState newState) {
        String coordinates = coordinateOrdering(event.getCoordinates());
        events.put(coordinates, event);
        eventsByHash.put(qb64(event.hash(DigestAlgorithm.DEFAULT)), coordinates);
        keyState.put(coordinates, newState);
        keyStateByIdentifier.put(qb64(event.getIdentifier()), coordinates);
        appendAttachments(event.getCoordinates(), event.getAuthentication(), event.getEndorsements(),
                          event.getReceipts());
    }

    public OptionalLong findLatestReceipt(Identifier forIdentifier, Identifier byIdentifier) {
        return OptionalLong.of(lastReceipt.get(receiptPrefix(forIdentifier, byIdentifier)));
    }

    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return Optional.of(events.get(coordinateOrdering(coordinates)));
    }

    public Optional<KeyState> getKeyState(Identifier identifier) {
        String stateHash = keyStateByIdentifier.get(qb64(identifier));

        return stateHash == null ? Optional.empty() : Optional.of(keyState.get(stateHash));
    }

    private void appendAttachments(EventCoordinates coordinates, Map<Integer, JohnHancock> signatures,
                                   Map<Integer, JohnHancock> receipts,
                                   Map<EventCoordinates, Map<Integer, JohnHancock>> otherReceipts) {
        String coords = coordinateOrdering(coordinates);
        authentications.put(coords, signatures(signatures));
        endorsements.put(coords, signatures(receipts));
        for (var otherReceipt : otherReceipts.entrySet()) {
            var key = receiptOrdering(coordinates, otherReceipt.getKey());
            this.receipts.put(key, signatures(otherReceipt.getValue()));
            lastReceipt.put(receiptPrefix(coordinates.getIdentifier(), otherReceipt.getKey().getIdentifier()),
                            coordinates.getSequenceNumber());
        }
    }
}
