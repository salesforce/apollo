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
import sandbox.java.sql.Ref;
import sandbox.java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class RefWrapper implements Ref {
    private final java.sql.Ref wrapped;

    public RefWrapper(java.sql.Ref wrapped) {
        this.wrapped = wrapped;
    }

    public String getBaseTypeName() throws SQLException {
        try {
            return String.toDJVM(wrapped.getBaseTypeName());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getObject(convertClassMap(map)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public Object getObject() throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getObject());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public void setObject(Object value) throws SQLException {
        try {
            wrapped.setObject(DJVM.unsandbox(value));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public java.sql.Ref toJsRef() {
        return wrapped;
    }
}
