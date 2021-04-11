/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;

import org.h2.Driver;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

/**
 * There may be multiple Java methods that match a function name. Each method
 * must have a different number of parameters however. This helper class
 * represents one such method.
 */
public class JavaMethod {
    private boolean      hasConnectionParam;
    private final Method method;
    private int          paramCount;
    private Class<?>     varArgClass;
    private boolean      varArgs;

    JavaMethod(Method method) {
        this.method = method;
        Class<?>[] paramClasses = method.getParameterTypes();
        paramCount = paramClasses.length;
        if (paramCount > 0) {
            Class<?> paramClass = paramClasses[0];
            if (Connection.class.isAssignableFrom(paramClass)) {
                hasConnectionParam = true;
                paramCount--;
            }
        }
        if (paramCount > 0) {
            Class<?> lastArg = paramClasses[paramClasses.length - 1];
            if (lastArg.isArray() && method.isVarArgs()) {
                varArgs = true;
                varArgClass = lastArg.getComponentType();
            }
        }
    }

    /**
     * Call the user-defined function and return the value.
     *
     * @param session the session
     * @param args    the argument list
     * @return the value
     */
    public Object getValue(Object instance, Session session, Value[] args) {
        Class<?>[] paramClasses = method.getParameterTypes();
        Object[] params = new Object[paramClasses.length];
        int p = 0;
        if (hasConnectionParam && params.length > 0) {
            params[p++] = session.createConnection(false);
        }

        // allocate array for varArgs parameters
        Object varArg = null;
        if (varArgs) {
            int len = args.length - params.length + 1 + (hasConnectionParam ? 1 : 0);
            varArg = Array.newInstance(varArgClass, len);
            params[params.length - 1] = varArg;
        }

        for (int a = 0, len = args.length; a < len; a++, p++) {
            boolean currentIsVarArg = varArgs && p >= paramClasses.length - 1;
            Class<?> paramClass;
            if (currentIsVarArg) {
                paramClass = varArgClass;
            } else {
                paramClass = paramClasses[p];
            }
            int type = DataType.getTypeFromClass(paramClass);
            Value v = args[a];
            Object o;
            if (Value.class.isAssignableFrom(paramClass)) {
                o = v;
            } else if (v.getValueType() == Value.ARRAY && paramClass.isArray()
                    && paramClass.getComponentType() != Object.class) {
                Value[] array = ((ValueArray) v).getList();
                Object[] objArray = (Object[]) Array.newInstance(paramClass.getComponentType(), array.length);
                int componentType = DataType.getTypeFromClass(paramClass.getComponentType());
                for (int i = 0; i < objArray.length; i++) {
                    objArray[i] = array[i].convertTo(componentType, session, false).getObject();
                }
                o = objArray;
            } else {
                v = v.convertTo(type, session, false);
                o = v.getObject();
            }
            if (o == null) {
                if (paramClass.isPrimitive()) {
                    // NULL for a java primitive: return NULL
                    return ValueNull.INSTANCE;
                }
            } else {
                if (!paramClass.isAssignableFrom(o.getClass()) && !paramClass.isPrimitive()) {
                    o = DataType.convertTo(session.createConnection(false), v, paramClass);
                }
            }
            if (currentIsVarArg) {
                Array.set(varArg, p - params.length + 1, o);
            } else {
                params[p] = o;
            }
        }
        Value identity = session.getLastScopeIdentity();
        boolean defaultConnection = session.getDatabase().getSettings().defaultConnection;
        try {
            session.setAutoCommit(false);
            try {
                if (defaultConnection) {
                    Driver.setDefaultConnection(session.createConnection(false));
                }
                return method.invoke(instance, params);
            } catch (InvocationTargetException e) {
                StringBuilder builder = new StringBuilder(method.getName()).append('(');
                for (int i = 0, length = params.length; i < length; i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    builder.append(params[i]);
                }
                builder.append(')');
                throw DbException.convertInvocation(e, builder.toString());
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        } finally {
            session.setLastScopeIdentity(identity);
            if (defaultConnection) {
                Driver.setDefaultConnection(null);
            }
        }
    }

    /**
     * Check if this function requires a database connection.
     *
     * @return if the function requires a connection
     */
    public boolean hasConnectionParam() {
        return this.hasConnectionParam;
    }

    @Override
    public String toString() {
        return method.toString();
    }
}
