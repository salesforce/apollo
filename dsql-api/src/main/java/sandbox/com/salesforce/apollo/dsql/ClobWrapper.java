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

import sandbox.java.lang.String;
import sandbox.java.sql.Clob;
import sandbox.java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class ClobWrapper implements Clob {
    private final java.sql.Clob wrapped;

    public ClobWrapper(java.sql.Clob wrapped) {
        this.wrapped = wrapped;
    }

    public void free() throws SQLException {
        try {
            wrapped.free();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public InputStream getAsciiStream() throws SQLException {
        try {
            return wrapped.getAsciiStream();
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

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        try {
            return wrapped.getCharacterStream(pos, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getSubString(long pos, int length) throws SQLException {
        try {
            return String.toDJVM(wrapped.getSubString(pos, length));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long length() throws SQLException {
        try {
            return wrapped.length();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long position(Clob searchstr, long start) throws SQLException {
        try {
            return wrapped.position(searchstr.toJsClob(), start);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long position(String searchstr, long start) throws SQLException {
        try {
            return wrapped.position(String.fromDJVM(searchstr), start);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
        try {
            return wrapped.setAsciiStream(pos);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Writer setCharacterStream(long pos) throws SQLException {
        try {
            return wrapped.setCharacterStream(pos);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int setString(long pos, String str) throws SQLException {
        try {
            return wrapped.setString(pos, String.fromDJVM(str));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
        try {
            return wrapped.setString(pos, String.fromDJVM(str), offset, len);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public java.sql.Clob toJsClob() {
        return wrapped;
    }

    public void truncate(long len) throws SQLException {
        try {
            wrapped.truncate(len);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

}
