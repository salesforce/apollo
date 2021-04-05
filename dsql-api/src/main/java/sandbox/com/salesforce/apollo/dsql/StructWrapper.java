/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import static sandbox.com.salesforce.apollo.dsql.ConnectionWrapper.convertClassMap;

import java.util.Map;

import sandbox.java.lang.DJVM;
import sandbox.java.lang.Object;
import sandbox.java.lang.String;
import sandbox.java.sql.SQLException;
import sandbox.java.sql.Struct;

/**
 * @author hal.hildebrand
 *
 */
public class StructWrapper implements Struct {

    private final java.sql.Struct wrapped;

    public StructWrapper(java.sql.Struct wrapped) {
        this.wrapped = wrapped;
    }

    public String getSQLTypeName() throws SQLException {
        try {
            return String.toDJVM(wrapped.getSQLTypeName());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object[] getAttributes() throws SQLException {
        try {
            return (Object[]) DJVM.sandbox(wrapped.getAttributes());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        try {
            return (Object[]) DJVM.sandbox(wrapped.getAttributes(convertClassMap(map)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public java.sql.Struct toJsStruct() {
        return wrapped;
    }
}
