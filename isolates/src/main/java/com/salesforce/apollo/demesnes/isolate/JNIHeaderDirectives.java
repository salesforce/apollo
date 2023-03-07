/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demesnes.isolate;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CFunctionPointer;
import org.graalvm.nativeimage.c.function.InvokeCFunctionPointer;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CPointerTo;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.word.PointerBase;

interface CallStaticVoidMethod extends CFunctionPointer {
    @InvokeCFunctionPointer
    void call(JNIEnvironment env, JClass cls, JMethodID methodid, JValue args);
}

interface GetByteArrayElements extends CFunctionPointer {
    @InvokeCFunctionPointer
    CCharPointer call(JNIEnvironment env, JByteArray array, boolean isCopy);
}

interface GetMethodId extends CFunctionPointer {
    @InvokeCFunctionPointer
    JMethodID find(JNIEnvironment env, JClass clazz, CCharPointer name, CCharPointer sig);
}

interface JByteArray extends PointerBase {
}

interface JClass extends PointerBase {
}

interface JMethodID extends PointerBase {
}

@CContext(JNIHeaderDirectives.class)
@CStruct(value = "JNIEnv_", addStructKeyword = true)
interface JNIEnvironment extends PointerBase {
    @CField("functions")
    JNINativeInterface getFunctions();
}

@CPointerTo(JNIEnvironment.class)
interface JNIEnvironmentPointer extends PointerBase {
    JNIEnvironment read();

    void write(JNIEnvironment value);
}

final class JNIHeaderDirectives implements CContext.Directives {
    private static final String PLATFORM;
    static {
        PLATFORM = "darwin"; // TODO determine platform
    }

    private static File[] findJNIHeaders() throws IllegalStateException {
        final File jreHome = new File(System.getProperty("java.home"));
        final File include = new File(jreHome, "include");
        final File[] jnis = { new File(include, "jni.h"), new File(new File(include, PLATFORM), "jni_md.h"), };
        return jnis;
    }

    @Override
    public List<String> getHeaderFiles() {
        File[] jnis = findJNIHeaders();
        return Arrays.asList("<" + jnis[0] + ">", "<" + jnis[1] + ">");
    }

    @Override
    public List<String> getOptions() {
        File[] jnis = findJNIHeaders();
        return Arrays.asList("-I" + jnis[0].getParent(), "-I" + jnis[1].getParent());
    }
}

@CContext(JNIHeaderDirectives.class)
@CStruct(value = "JNINativeInterface_", addStructKeyword = true)
interface JNINativeInterface extends PointerBase {
    @CField
    CallStaticVoidMethod getCallStaticVoidMethodA();

    @CField
    GetByteArrayElements getGetByteArrayElements();

    @CField
    GetMethodId getGetStaticMethodID();

}

interface JObject extends PointerBase {
}

@CContext(JNIHeaderDirectives.class)
@CStruct("jvalue")
interface JValue extends PointerBase {
    JValue addressOf(int index);

    @CField
    byte b();

    @CField
    void b(byte b);

    @CField
    char c();

    @CField
    void c(char ch);

    @CField
    double d();

    @CField
    void d(double d);

    @CField
    float f();

    @CField
    void f(float f);

    @CField
    int i();

    @CField
    void i(int i);

    @CField
    long j();

    @CField
    void j(long l);

    @CField
    JObject l();

    @CField
    void l(JObject obj);

    @CField
    short s();

    @CField
    void s(short s);

    @CField
    boolean z();

    @CField
    void z(boolean b);
}
