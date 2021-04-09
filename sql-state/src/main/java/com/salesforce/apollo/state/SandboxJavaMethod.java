/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.value.Value;

import net.corda.djvm.SandboxRuntimeContext;

/**
 * @author hal.hildebrand
 *
 */
public class SandboxJavaMethod implements Comparable<SandboxJavaMethod> {
    private final SandboxRuntimeContext context;
    private final MethodHandle          getColumnClasses;
    private final MethodHandle          getDataType;
    private final MethodHandle          getParameterCount;
    private final MethodHandle          getValue;
    private final MethodHandle          hasConnectionParam;
    private final int                   id;
    private final MethodHandle          invoke;
    private final MethodHandle          isVarArgs;
    private final MethodHandle          toString;
    private final Object                wrapped;

    public SandboxJavaMethod(SandboxRuntimeContext context, Object wrapped, int id) throws Exception {
        this.context = context;
        this.wrapped = wrapped;
        this.id = id;
        Class<? extends Object> wrapperClass = wrapped.getClass();
        invoke = MethodHandles.lookup()
                              .findVirtual(wrapperClass, "invoke",
                                           MethodType.methodType(Object.class, Object.class, Session.class,
                                                                 new Value[0].getClass()));
        getValue = MethodHandles.lookup()
                                .findVirtual(wrapperClass, "getValue",
                                             MethodType.methodType(Value.class, Session.class,
                                                                   new Expression[0].getClass(), boolean.class));
        getColumnClasses = MethodHandles.lookup()
                                        .findVirtual(wrapperClass, "getColumnClasses",
                                                     MethodType.methodType(new Class<?>[0].getClass()));
        getParameterCount = MethodHandles.lookup()
                                         .findVirtual(wrapperClass, "getParameterCount",
                                                      MethodType.methodType(int.class));
        getDataType = MethodHandles.lookup().findVirtual(wrapperClass, "getDataType", MethodType.methodType(int.class));
        isVarArgs = MethodHandles.lookup().findVirtual(wrapperClass, "isVarArgs", MethodType.methodType(boolean.class));
        hasConnectionParam = MethodHandles.lookup()
                                          .findVirtual(wrapperClass, "hasConnectionParam",
                                                       MethodType.methodType(boolean.class));
        toString = MethodHandles.lookup().findVirtual(wrapperClass, "toString", MethodType.methodType(String.class));
    }

    @Override
    public int compareTo(SandboxJavaMethod m) {
        boolean varArgs = isVarArgs();
        if (varArgs != m.isVarArgs()) {
            return varArgs ? 1 : -1;
        }
        int paramCount = getParameterCount();
        int mParamCount = m.getParameterCount();
        if (paramCount != mParamCount) {
            return paramCount - mParamCount;
        }
        boolean hasConnP = hasConnectionParam();
        if (hasConnP != m.hasConnectionParam()) {
            return hasConnP ? 1 : -1;
        }
        return getId() - m.getId();
    }

    public Class<?>[] getColumnClasses() {
        AtomicReference<Class<?>[]> returned = new AtomicReference<>();
        context.use(ctx -> {
            try {
                returned.set((Class<?>[]) getColumnClasses.invokeWithArguments(wrapped));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to invoke method ", e);
            }
        });
        return returned.get();
    }

    public int getDataType() {
        AtomicInteger returned = new AtomicInteger();
        context.use(ctx -> {
            try {
                returned.set((Integer) getDataType.invokeWithArguments(wrapped));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to invoke method ", e);
            }
        });
        return returned.get();
    }

    public int getId() {
        return id;
    }

    public int getParameterCount() {
        AtomicInteger returned = new AtomicInteger();
        context.use(ctx -> {
            try {
                returned.set((Integer) getParameterCount.invokeWithArguments(wrapped));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to invoke method ", e);
            }
        });
        return returned.get();
    }

    public Value getValue(Session session, Expression[] args, boolean columnList) {
        AtomicReference<Value> returned = new AtomicReference<>();
        context.use(ctx -> {
            try {
                returned.set((Value) getValue.invokeWithArguments(wrapped, session, args, columnList));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to invoke method ", e);
            }
        });
        return returned.get();
    }

    public boolean hasConnectionParam() {
        AtomicBoolean returned = new AtomicBoolean();
        context.use(ctx -> {
            try {
                returned.set((Boolean) hasConnectionParam.invokeWithArguments(wrapped));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to invoke method ", e);
            }
        });
        return returned.get();
    }

    public Object invoke(Object receiver, Session session, Value[] args) {
        AtomicReference<Object> returned = new AtomicReference<>();
        context.use(ctx -> {
            try {
                returned.set(invoke.invokeWithArguments(wrapped, receiver, session, args));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to invoke method ", e);
            }
        });
        return returned.get();
    }

    public boolean isVarArgs() {
        AtomicBoolean returned = new AtomicBoolean();
        context.use(ctx -> {
            try {
                returned.set((Boolean) isVarArgs.invokeWithArguments(wrapped));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to invoke method ", e);
            }
        });
        return returned.get();
    }

    public String toString() {
        AtomicReference<String> returned = new AtomicReference<>();
        context.use(ctx -> {
            try {
                returned.set((String) toString.invokeWithArguments(wrapped));
            } catch (Throwable e) {
                throw new IllegalStateException("Unable to invoke method ", e);
            }
        });
        return returned.get();
    }
}
