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

    private static native long createIsolate();

    private static native boolean launch(long isolateId, byte[] parameters, int paramsSize, byte[] password,
                                         int pwdLength);

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
                DemesneParameters p;
                try {
                    p = DemesneParameters.parseFrom(serialized);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException("Cannot deserialize parameters", e);
                }
                log.error("Launching Demesne JNI Bridge, param len: {} pwd len: {} serialized: {}", serialized.length,
                          password.length, p);
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
