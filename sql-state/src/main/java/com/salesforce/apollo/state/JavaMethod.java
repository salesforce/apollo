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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import deterministic.org.h2.Driver;
import deterministic.org.h2.engine.SessionLocal;
import deterministic.org.h2.expression.Alias;
import deterministic.org.h2.expression.Expression;
import deterministic.org.h2.expression.ExpressionColumn;
import deterministic.org.h2.jdbc.JdbcConnection;
import deterministic.org.h2.message.DbException;
import deterministic.org.h2.result.LocalResult;
import deterministic.org.h2.result.ResultInterface;
import deterministic.org.h2.table.Column;
import deterministic.org.h2.util.Utils;
import deterministic.org.h2.value.DataType;
import deterministic.org.h2.value.TypeInfo;
import deterministic.org.h2.value.Value;
import deterministic.org.h2.value.ValueNull;
import deterministic.org.h2.value.ValueToObjectConverter;

/**
 * There may be multiple Java methods that match a function name. Each method
 * must have a different number of parameters however. This helper class
 * represents one such method.
 */
public class JavaMethod implements Comparable<JavaMethod> {
    /**
     * Create a result for the given result set.
     *
     * @param session the session
     * @param rs      the result set
     * @param maxrows the maximum number of rows to read (0 to just read the meta
     *                data)
     * @return the value
     */
    public static ResultInterface resultSetToResult(SessionLocal session, ResultSet rs, int maxrows) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            Expression[] columns = new Expression[columnCount];
            for (int i = 0; i < columnCount; i++) {
                String alias = meta.getColumnLabel(i + 1);
                String name = meta.getColumnName(i + 1);
                String columnTypeName = meta.getColumnTypeName(i + 1);
                int columnType = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1), columnTypeName);
                int precision = meta.getPrecision(i + 1);
                int scale = meta.getScale(i + 1);
                TypeInfo typeInfo;
                if (columnType == Value.ARRAY && columnTypeName.endsWith(" ARRAY")) {
                    typeInfo = TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0,
                                                    TypeInfo.getTypeInfo(DataType.getTypeByName(columnTypeName.substring(0,
                                                                                                                         columnTypeName.length()
                                                                                                                         - 6),
                                                                                                session.getMode()).type));
                } else {
                    typeInfo = TypeInfo.getTypeInfo(columnType, precision, scale, null);
                }
                Expression e = new ExpressionColumn(session.getDatabase(), new Column(name, typeInfo));
                if (!alias.equals(name)) {
                    e = new Alias(e, alias, false);
                }
                columns[i] = e;
            }
            LocalResult result = new LocalResult(session, columns, columnCount, columnCount);
            for (int i = 0; i < maxrows && rs.next(); i++) {
                Value[] list = new Value[columnCount];
                for (int j = 0; j < columnCount; j++) {
                    list[j] = ValueToObjectConverter.objectToValue(session, rs.getObject(j + 1),
                                                                   columns[j].getType().getValueType());
                }
                result.addRow(list);
            }
            result.done();
            return result;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private boolean      hasConnectionParam;
    private final Method method;
    private int          paramCount;
    private Class<?>     varArgClass;

    private boolean varArgs;

    JavaMethod(Method method) {
        assert method != null;
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

    @Override
    public int compareTo(JavaMethod m) {
        if (varArgs != m.varArgs) {
            return varArgs ? 1 : -1;
        }
        if (paramCount != m.paramCount) {
            return paramCount - m.paramCount;
        }
        if (hasConnectionParam != m.hasConnectionParam) {
            return hasConnectionParam ? 1 : -1;
        }
        return 0;
    }

    public Class<?>[] getColumnClasses() {
        return method.getParameterTypes();
    }

    public int getParameterCount() {
        return paramCount;
    }

    /**
     * Call the user-defined function and return the value.
     *
     * @param session    the session
     * @param args       the argument list
     * @param columnList true if the function should only return the column list
     * @return the value
     */
    public Object getValue(Object instance, SessionLocal session, Value[] args) {
        Class<?>[] paramClasses = method.getParameterTypes();
        Object[] params = new Object[paramClasses.length];
        int p = 0;
        JdbcConnection conn = session.createConnection(false);
        if (hasConnectionParam && params.length > 0) {
            params[p++] = conn;
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
            Value v = args[a];
            Object o;
            if (Value.class.isAssignableFrom(paramClass)) {
                o = v;
            } else {
                boolean primitive = paramClass.isPrimitive();
                if (v == ValueNull.INSTANCE) {
                    if (primitive) {
                        // NULL for a java primitive: return NULL
                        return null;
                    } else {
                        o = null;
                    }
                } else {
                    o = ValueToObjectConverter.valueToObject((Class<?>) (primitive ? Utils.getNonPrimitiveClass(paramClass)
                                                                                   : paramClass),
                                                             v, conn);
                }
            }
            if (currentIsVarArg) {
                Array.set(varArg, p - params.length + 1, o);
            } else {
                params[p] = o;
            }
        }
        boolean old = session.getAutoCommit();
        Value identity = session.getLastIdentity();
        boolean defaultConnection = session.getDatabase().getSettings().defaultConnection;
        try {
            session.setAutoCommit(false);
            Object returnValue;
            try {
                if (defaultConnection) {
                    Driver.setDefaultConnection(session.createConnection(false));
                }
                returnValue = method.invoke(instance, params);
                if (returnValue == null) {
                    return null;
                }
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
            return returnValue;
        } finally {
            session.setLastIdentity(identity);
            session.setAutoCommit(old);
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

    public boolean isVarArgs() {
        return varArgs;
    }

    @Override
    public String toString() {
        return method.toString();
    }
}
