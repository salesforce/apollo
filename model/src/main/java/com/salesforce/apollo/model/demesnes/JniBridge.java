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
import java.util.Arrays;
import java.util.List;

import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.demesne.proto.ViewChange;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.RotationEvent;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.EventCoordinates;

/**
 * Interface to SubDomain Demesne running in the GraalVM Isolate as JNI library
 *
 * @author hal.hildebrand
 *
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

    private static native boolean active(long isolateId);

    private static native void commit(long isolateId, byte[] eventCoordinates, int eventCoordinatesLen);

    private static native long createIsolate();

    private static native byte[] inception(long isolateId, byte[] identifier, int identifierLen);

    private static native boolean launch(long isolateId, byte[] parameters, int paramLen, byte[] password, int passLen);

    private static native byte[] rotate(long isolateId);

    private static native void start(long isolateId);

    private static native void stop(long isolateId);

    private static byte[] toBytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        return byteBuffer.array();
    }

    private static native boolean viewChange(long isolateId, byte[] parameters, int paramLength);

    private final long isolateId;

    public JniBridge(DemesneParameters parameters, char[] password) {
        try {
            final byte[] bytes = toBytes(password);
            try {
                isolateId = createIsolate();
                final var serialized = parameters.toByteString().toByteArray();
                launch(isolateId, serialized, serialized.length, bytes, bytes.length);
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        } finally {
            Arrays.fill(password, ' ');
        }
    }

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
    public InceptionEvent inception(Ident identifier) {
        final var ident = identifier.toByteString().toByteArray();
        var bytes = inception(isolateId, ident, ident.length);
        try {
            return InceptionEvent.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserializing inception event", e);
            return InceptionEvent.getDefaultInstance();
        }
    }

    @Override
    public RotationEvent rotate() {
        var bytes = rotate(isolateId);
        try {
            return RotationEvent.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserializing inception event", e);
            return RotationEvent.getDefaultInstance();
        }
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
