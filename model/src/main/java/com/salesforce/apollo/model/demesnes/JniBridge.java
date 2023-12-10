/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.demesne.proto.DemesneParameters;
import com.salesforce.apollo.demesne.proto.ViewChange;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

/**
 * Interface to SubDomain Demesne running in the GraalVM Isolate as JNI library
 *
 * @author hal.hildebrand
 */
public class JniBridge implements Demesne {
    private static final String DEMESNE_SHARED_LIB_NAME = "demesne";
    private static final Logger log                     = LoggerFactory.getLogger(JniBridge.class);

    static {
        try {
            NativeLoader.loadLibrary(DEMESNE_SHARED_LIB_NAME);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load shared library: " + DEMESNE_SHARED_LIB_NAME, e);
        }
    }

    private final long isolateId;

    public JniBridge(DemesneParameters parameters) {
        isolateId = createIsolate();
        final var serialized = parameters.toByteString().toByteArray();
        launch(isolateId, serialized, serialized.length);
    }

    private static native boolean active(long isolateId);

    private static native void commit(long isolateId, byte[] eventCoordinates, int eventCoordinatesLen);

    private static native long createIsolate();

    private static native byte[] id(long isolateId);

    private static native byte[] inception(long isolateId, byte[] identifier, int identifierLen, byte[] spec,
                                           int specLen);

    private static native boolean launch(long isolateId, byte[] parameters, int paramLen);

    private static native byte[] rotate(long isolateId, byte[] spec, int specLen);

    private static native void start(long isolateId);

    private static native void stop(long isolateId);

    private static byte[] toBytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        return byteBuffer.array();
    }

    private static native boolean viewChange(long isolateId, byte[] parameters, int paramLength);

    @Override
    public boolean active() {
        return active(isolateId);
    }

    @Override
    public void commit(EventCoords coordinates) {
        var bytes = coordinates.toByteArray();
        commit(isolateId, bytes, bytes.length);
    }

    @Override
    public SelfAddressingIdentifier getId() {
        final var bytes = id(isolateId);
        try {
            return (SelfAddressingIdentifier) Identifier.from(Ident.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Cannot get sub domain id", e);
        }
    }

    @Override
    public DelegatedInceptionEvent inception(Ident identifier, Builder<SelfAddressingIdentifier> specification) {
        final var ident = identifier.toByteString().toByteArray();
        final var spec = specification.toSpec().toByteArray();
        var bytes = inception(isolateId, ident, ident.length, spec, spec.length);
        return (DelegatedInceptionEvent) ProtobufEventFactory.toKeyEvent(bytes, KeyEvent.DELEGATED_INCEPTION_TYPE);
    }

    @Override
    public DelegatedRotationEvent rotate(RotationSpecification.Builder specification) {
        final var spec = specification.toSpec().toByteString().toByteArray();
        var bytes = rotate(isolateId, spec, spec.length);
        return (DelegatedRotationEvent) ProtobufEventFactory.toKeyEvent(bytes, KeyEvent.DELEGATED_ROTATION_TYPE);
    }

    @Override
    public void start() {
        start(isolateId);
    }

    @Override
    public void stop() {
        start(isolateId);
    }

    @Override
    public void viewChange(Digest viewId, List<EventCoordinates> joining, List<Digest> leaving) {
        var bytes = ViewChange.newBuilder().build().toByteArray();
        viewChange(isolateId, bytes, bytes.length);
    }
}
