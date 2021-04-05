/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import sandbox.java.lang.String;
import sandbox.java.sql.ParameterMetaData;
import sandbox.java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class ParameterMetaDataWrapper implements ParameterMetaData {

    private final java.sql.ParameterMetaData wrapped;

    public ParameterMetaDataWrapper(java.sql.ParameterMetaData wrapped) {
        this.wrapped = wrapped;
    }

    public String getParameterClassName(int param) throws SQLException {
        try {
            return String.toDJVM(wrapped.getParameterClassName(param));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getParameterCount() throws SQLException {
        try {
            return wrapped.getParameterCount();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getParameterMode(int param) throws SQLException {
        try {
            return wrapped.getParameterMode(param);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getParameterType(int param) throws SQLException {
        try {
            return wrapped.getParameterType(param);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getParameterTypeName(int param) throws SQLException {
        try {
            return String.toDJVM(wrapped.getParameterTypeName(param));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getPrecision(int param) throws SQLException {
        try {
            return wrapped.getPrecision(param);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getScale(int param) throws SQLException {
        try {
            return wrapped.getScale(param);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int isNullable(int param) throws SQLException {
        try {
            return wrapped.isNullable(param);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isSigned(int param) throws SQLException {
        try {
            return wrapped.isSigned(param);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            return wrapped.isWrapperFor(iface);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return wrapped.unwrap(iface);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

}
