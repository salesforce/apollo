/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;

import org.h2.mvstore.MVStore;

import com.google.protobuf.Any;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class VirtualMachine {

    private final Context<Member> context;
    private final ClassLoader     domain;
    private final Method          register;
    private final MVStore         store;
    private Object                tramp;

    public VirtualMachine(Context<Member> context, ClassLoader domain, MVStore store) {
        this.context = context;
        this.domain = domain;
        this.store = store;
        try {
            tramp = domain.loadClass("com.apollo.vm.EventTrampoline").getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Unable to load Event Trampoline", e);
        }
        try {
            register = tramp.getClass().getDeclaredMethod("register", new Class[] { String.class, BiConsumer.class });
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Unable to load Event Trampoline register method", e);
        }
    }

    public void register(String event, BiConsumer<String, Any> handler) {
        try {
            register.invoke(tramp, event, handler);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new IllegalStateException("Cannot register the handler for " + event, e);
        }
    }
}
