/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import java.io.InputStream;
import java.io.OutputStream;

import sandbox.java.sql.Blob;
import sandbox.java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class BlobWrapper implements Blob {

    private final java.sql.Blob wrapped;

    public BlobWrapper(java.sql.Blob wrapped) {
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

    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        try {
            return wrapped.getBinaryStream(pos, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        try {
            return wrapped.getBytes(pos, length);
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

    public long position(Blob pattern, long start) throws SQLException {
        try {
            return wrapped.position(pattern.toJsBlob(), start);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long position(byte[] pattern, long start) throws SQLException {
        try {
            return wrapped.position(pattern, start);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public OutputStream setBinaryStream(long pos) throws SQLException {
        try {
            return wrapped.setBinaryStream(pos);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException {
        try {
            return wrapped.setBytes(pos, bytes);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        try {
            return wrapped.setBytes(pos, bytes, offset, len);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public java.sql.Blob toJsBlob() {
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
