/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hal.hildebrand
 *
 */
public class ProxyAdapter implements InvocationHandler {
    private static Map<Method, MethodHandle> handlesOf(Class<?> clazz) {
        Class<?> outer = outerOf(clazz);
        Map<Method, MethodHandle> handles = new HashMap<>();
        for (Method original : outer.getDeclaredMethods()) {
            Class<?>[] params = new Class[original.getParameterCount()];
            for (int i = 0; i < original.getParameterCount(); i++) {
                params[i] = innerOf(original.getParameterTypes()[i]);
            }
            try {
                handles.put(original,
                            MethodHandles.lookup()
                                         .findVirtual(clazz, original.getName(),
                                                      MethodType.methodType(void.class, params)));
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new IllegalStateException("unable to find matching handler for: " + original);
            }
        }
        return handles;
    }

    private static Class<?> innerOf(Class<?> clazz) {
        String innerName = clazz.getCanonicalName().substring("sandbox.".length());
        try {
            return clazz.getClassLoader().loadClass(innerName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unable to find class: " + innerName);
        }
    }

    private static Class<?> outerOf(Class<?> clazz) {
        String outerName = "sandbox." + clazz.getCanonicalName();
        try {
            return clazz.getClassLoader().loadClass(outerName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unable to find class: " + outerName);
        }
    }

    private final Map<Method, MethodHandle> handles;
    private final Object                    nat;

    public ProxyAdapter(java.lang.Object nat) {
        this.nat = nat;
        handles = handlesOf(nat.getClass());
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        MethodHandle handle = handles.get(method);
        if (handle == null) {
            throw new IllegalStateException("Cannot adapt invocation of: " + method);
        }
        Object returned;
        try {
            returned = handle.invoke(convert(args));
        } catch (Throwable e) {
            sandbox.java.sql.SQLException cvtd = convert(e);
            if (cvtd != null) {
                throw cvtd;
            }
            throw e;
        }

        return returned == null ? null : wrap(returned);
    }

    private Object wrap(Object value) {
        return null;
    }

    private Object[] convert(Object[] args) {
        Object[] list = new Object[args.length + 1];
        list[0] = nat;
        for (int i = 1; i <= args.length; i++) {
            list[i] = args[i - 1];
        }
        return list;
    }

    private sandbox.java.sql.SQLException convert(Throwable t) {
        if (!(t instanceof SQLException)) {
            return null;
        }
        SQLException e = (SQLException) t;
        return new sandbox.java.sql.SQLException(sandbox.java.lang.String.toDJVM(e.getMessage()),
                sandbox.java.lang.String.toDJVM(e.getSQLState()), e.getErrorCode());
    }
}
