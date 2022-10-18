/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.domain;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.ObjectHandle;
import org.graalvm.nativeimage.ObjectHandles;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

/**
 * @author hal.hildebrand
 *
 */
public class Demesne {
    @CEntryPoint
    public static ObjectHandle launch(@CEntryPoint.IsolateThreadContext IsolateThread renderingContext,
                                      CCharPointer cString) {
        /* Convert the C string to the target Java string. */
        String targetString = CTypeConversion.toJavaString(cString);
        /*
         * Encapsulate the target string in a handle that can be returned back to the
         * source isolate.
         */
        return ObjectHandles.getGlobal().create(targetString);
    }
}
