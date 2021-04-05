/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import javax.xml.transform.Result;
import javax.xml.transform.Source;

import sandbox.java.lang.String;
import sandbox.java.sql.SQLException;
import sandbox.java.sql.SQLXML;

/**
 * @author hal.hildebrand
 *
 */
public class SQLXMLWrapper implements SQLXML {
    private final java.sql.SQLXML wrapped;

    public SQLXMLWrapper(java.sql.SQLXML wrapped) {
        this.wrapped = wrapped;
    }

    public void free() throws SQLException {
        try {
            wrapped.free();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public InputStream getBinaryStream() throws SQLException {
        try {
            return wrapped.getBinaryStream();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Reader getCharacterStream() throws SQLException {
        try {
            return wrapped.getCharacterStream();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        try {
            return wrapped.getSource(sourceClass);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getString() throws SQLException {
        try {
            return String.toDJVM(wrapped.getString());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public OutputStream setBinaryStream() throws SQLException {
        try {
            return wrapped.setBinaryStream();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Writer setCharacterStream() throws SQLException {
        try {
            return wrapped.setCharacterStream();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        try {
            return wrapped.setResult(resultClass);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setString(String value) throws SQLException {
        try {
            wrapped.setString(String.fromDJVM(value));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public java.sql.SQLXML toJsSQLXML() {
        return wrapped;
    }
}
