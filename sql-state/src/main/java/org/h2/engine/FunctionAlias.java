/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package org.h2.engine;

import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObjectBase;
import org.h2.table.Table;
import org.h2.util.StringUtils;

import com.salesforce.apollo.state.DeterministicCompiler;
import com.salesforce.apollo.state.SandboxJavaMethod;

/**
 * Represents a user-defined function, or alias.
 *
 * @author Thomas Mueller
 * @author Gary Tong
 * @author hal.hildebrand - modified for DJVM integration
 */
public class FunctionAlias extends SchemaObjectBase {

    private String className;
    private String methodName;
    private String source;
    private SandboxJavaMethod[] javaMethods;
    private boolean deterministic;

    private FunctionAlias(Schema schema, int id, String name) {
        super(schema, id, name, Trace.FUNCTION);
    }

    /**
     * Create a new alias based on a method name.
     *
     * @param schema the schema
     * @param id the id
     * @param name the name
     * @param javaClassMethod the class and method name
     * @param force create the object even if the class or method does not exist
     * @return the database object
     */
    public static FunctionAlias newInstance(
            Schema schema, int id, String name, String javaClassMethod,
            boolean force) {
        FunctionAlias alias = new FunctionAlias(schema, id, name);
        int paren = javaClassMethod.indexOf('(');
        int lastDot = javaClassMethod.lastIndexOf('.', paren < 0 ?
                javaClassMethod.length() : paren);
        if (lastDot < 0) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, javaClassMethod);
        }
        alias.className = javaClassMethod.substring(0, lastDot);
        alias.methodName = javaClassMethod.substring(lastDot + 1);
        alias.init(force);
        return alias;
    }

    /**
     * Create a new alias based on source code.
     *
     * @param schema the schema
     * @param id the id
     * @param name the name
     * @param source the source code
     * @param force create the object even if the class or method does not exist
     * @return the database object
     */
    public static FunctionAlias newInstanceFromSource(
            Schema schema, int id, String name, String source, boolean force) {
        FunctionAlias alias = new FunctionAlias(schema, id, name);
        alias.source = source;
        alias.init(force);
        return alias;
    }

    private void init(boolean force) {
        try {
            // at least try to compile the class, otherwise the data type is not
            // initialized if it could be
            load();
        } catch (DbException e) {
            if (!force) {
                throw e;
            }
        }
    }

    private synchronized void load() {
        if (javaMethods != null) {
            return;
        }
        if (source != null) {
            loadFromSource();
        } else {
            loadClass();
        }
    }

    private void loadFromSource() {
        DeterministicCompiler compiler = database.getCompiler();
        compiler.loadFunctionFromSource(getName(), source);
        synchronized (compiler) {
            String fullClassName = Constants.USER_PACKAGE + "." + getName();
            compiler.loadFunctionFromSource(fullClassName, fullClassName);
            try {
                SandboxJavaMethod m = compiler.getMethod(fullClassName); 
                javaMethods = new SandboxJavaMethod[] {
                        m
                };
            } catch (DbException e) {
                throw e;
            } catch (Exception e) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_1, e, source);
            }
        }
    }

    private void loadClass() {
        DeterministicCompiler compiler = database.getCompiler();
        javaMethods = compiler.loadFunction(className, methodName);
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        return "DROP ALIAS IF EXISTS " + getSQL(true);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        // TODO can remove this method once FUNCTIONS_IN_SCHEMA is enabled
        if (database.getSettings().functionsInSchema || getSchema().getId() != Constants.MAIN_SCHEMA_ID) {
            return super.getSQL(builder, alwaysQuote);
        }
        return Parser.quoteIdentifier(builder, getName(), alwaysQuote);
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("CREATE FORCE ALIAS ");
        buff.append(getSQL(true));
        if (deterministic) {
            buff.append(" DETERMINISTIC");
        }
        if (source != null) {
            buff.append(" AS ");
            StringUtils.quoteStringSQL(buff, source);
        } else {
            buff.append(" FOR ");
            Parser.quoteIdentifier(buff, className + "." + methodName, true);
        }
        return buff.toString();
    }

    @Override
    public int getType() {
        return DbObject.FUNCTION_ALIAS;
    }

    @Override
    public synchronized void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
        className = null;
        methodName = null;
        javaMethods = null;
        invalidate();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("RENAME");
    }

    /**
     * Find the Java method that matches the arguments.
     *
     * @param args the argument list
     * @return the Java method
     * @throws DbException if no matching method could be found
     */
    public SandboxJavaMethod findJavaMethod(Expression[] args) {
        load();
        int parameterCount = args.length;
        for (SandboxJavaMethod m : javaMethods) {
            int count = m.getParameterCount();
            if (count == parameterCount || (m.isVarArgs() &&
                    count <= parameterCount + 1)) {
                return m;
            }
        }
        throw DbException.get(ErrorCode.METHOD_NOT_FOUND_1, getName() + " (" +
                className + ", parameter count: " + parameterCount + ")");
    }

    public String getJavaClassName() {
        return this.className;
    }

    public String getJavaMethodName() {
        return this.methodName;
    }

    /**
     * Get the Java methods mapped by this function.
     *
     * @return the Java methods.
     */
    public SandboxJavaMethod[] getJavaMethods() {
        load();
        return javaMethods;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public String getSource() {
        return source;
    }
}
