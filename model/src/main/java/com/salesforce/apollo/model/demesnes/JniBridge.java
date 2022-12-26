/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes;

import java.io.IOException;
import java.util.List;

import org.scijava.nativelib.NativeLoader;

import com.salesfoce.apollo.demesne.proto.DemesneParameters;
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

    static {
        try {
            NativeLoader.loadLibrary(DEMESNE_SHARED_LIB_NAME);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load shared library: " + DEMESNE_SHARED_LIB_NAME, e);
        }
    }

    private static native boolean active(long isolateId);

    private static native long createIsolate();

    private static native void launch(long isolateId, byte[] parameters, char[] ksPassword);

    private static native void start(long isolateId);

    private static native void stop(long isolateId);

    private static native void viewChange(long isolateId, byte[] viewId, byte[][] joining, byte[][] leaving);

    private final long isolateId;

    public JniBridge(DemesneParameters parameters, char[] password) {
        isolateId = createIsolate();
        launch(isolateId, parameters.toByteArray(), password);
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
        viewChange(isolateId, viewId.toDigeste().toByteArray(),
                   (byte[][]) joining.stream().map(coords -> coords.toEventCoords().toByteArray()).toArray(),
                   (byte[][]) leaving.stream().map(d -> d.toDigeste().toByteArray()).toArray());
    }
}
