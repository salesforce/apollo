package sandbox.com.salesforce.apollo.dsql;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.h2.Driver;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

import sandbox.java.lang.DJVM;
import sandbox.java.sql.Connection;
import sandbox.java.sql.ResultSet;

/**
 * There may be multiple Java methods that match a function name. Each method
 * must have a different number of parameters however. This helper class
 * represents one such method.
 */
public class JavaMethod {
    private static final RowSetFactory factory;
    static {
        try {
            factory = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create row set factory", e);
        }
    }
    private final int    dataType;
    private boolean      hasConnectionParam;
    private final Method method;
    private int          paramCount;

    private Class<?> varArgClass;

    private boolean varArgs;

    public JavaMethod(Method method) {
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
        Class<?> returnClass = method.getReturnType();
        try {
            dataType = returnClass.isPrimitive() ? DataType.getTypeFromClass(returnClass)
                    : DataType.getTypeFromClass(DJVM.fromDJVMType(returnClass));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("missing non sandbox equivalent class", e);
        }
    }

    public Class<?>[] getColumnClasses() {
        Class<?>[] nonSandboxed = new Class<?>[method.getParameterCount()];
        int i = 0;
        for (Class<?> clazz : method.getParameterTypes()) {
            try {
                nonSandboxed[i++] = clazz.isPrimitive() ? clazz : DJVM.fromDJVMType(clazz);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("missing non sandbox equivalent class", e);
            }
        }
        return nonSandboxed;
    }

    public int getDataType() {
        return dataType;
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
    public Value getValue(Session session, Expression[] args, boolean columnList) {
        return getValue(session, args, columnList);
    }

    /**
     * Check if this function requires a database connection.
     *
     * @return if the function requires a connection
     */
    public boolean hasConnectionParam() {
        return this.hasConnectionParam;
    }

    /**
     * Invoke the script with the arguments;
     * 
     * @param receiver - script instance
     * @param args     - script arguments
     */
    public Object invoke(Object receiver, Session session, Value[] args) {
        Class<?>[] paramClasses = method.getParameterTypes();
        Object[] params = new Object[paramClasses.length];
        int p = 0;
        if (hasConnectionParam && params.length > 0) {
            params[p++] = new ConnectionWrapper(session.createConnection(false));
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

            Class<?> nonSandboxParamClass;
            try {
                nonSandboxParamClass = DJVM.fromDJVMType(paramClass);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("missing non sandbox equivalent class", e);
            }
            int type;
            type = DataType.getTypeFromClass(nonSandboxParamClass);
            Value v = args[a];
            Object o;
            Class<?> valueClass;
            try {
                valueClass = DJVM.toDJVMType(Value.class);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Cannot find sandbox class for value", e);
            }
            if (valueClass.isAssignableFrom(paramClass)) {
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
                try {
                    o = DJVM.sandbox(v.getObject());
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("missing non sandbox equivalent class", e);
                }
            }
            if (o == null) {
                if (paramClass.isPrimitive()) {
                    // NULL for a java primitive: return NULL
                    return ValueNull.INSTANCE;
                }
            } else {
                if (!paramClass.isAssignableFrom(o.getClass()) && !paramClass.isPrimitive()) {
                    try {
                        o = DJVM.sandbox(DataType.convertTo(session.createConnection(false), v, nonSandboxParamClass));
                    } catch (ClassNotFoundException e) {
                        throw new IllegalStateException("missing non sandbox equivalent class", e);
                    }
                }
            }
            if (currentIsVarArg) {
                Array.set(varArg, p - params.length + 1, o);
            } else {
                params[p] = o;
            }
        }
        boolean old = session.getAutoCommit();
        Value identity = session.getLastScopeIdentity();
        boolean defaultConnection = session.getDatabase().getSettings().defaultConnection;
        try {
            session.setAutoCommit(false);
            Object returnValue;
            try {
                if (defaultConnection) {
                    Driver.setDefaultConnection(session.createConnection(false));
                }
                returnValue = method.invoke(receiver, params);
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
            if (returnValue instanceof ResultSet) {
                try {
                    CachedRowSet rowset = factory.createCachedRowSet();

                    rowset.populate(((ResultSet) returnValue).toJsResultSet());
                    return rowset;
                } catch (SQLException e) {
                    throw new IllegalStateException("Unable to convert sandboxed rowset", e);
                }
            }
            return returnValue;
        } finally {
            session.setLastScopeIdentity(identity);
            session.setAutoCommit(old);
            if (defaultConnection) {
                Driver.setDefaultConnection(null);
            }
        }
    }

    public boolean isVarArgs() {
        return varArgs;
    }

    @Override
    public String toString() {
        return method.toString();
    }

}
