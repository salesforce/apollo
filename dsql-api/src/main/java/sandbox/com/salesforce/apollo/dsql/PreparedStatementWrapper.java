/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Calendar;

import sandbox.java.lang.Object;
import sandbox.java.lang.String;
import sandbox.java.sql.Array;
import sandbox.java.sql.Blob;
import sandbox.java.sql.Clob;
import sandbox.java.sql.Connection;
import sandbox.java.sql.Date;
import sandbox.java.sql.NClob;
import sandbox.java.sql.ParameterMetaData;
import sandbox.java.sql.PreparedStatement;
import sandbox.java.sql.Ref;
import sandbox.java.sql.ResultSet;
import sandbox.java.sql.ResultSetMetaData;
import sandbox.java.sql.RowId;
import sandbox.java.sql.SQLException;
import sandbox.java.sql.SQLXML;
import sandbox.java.sql.Time;
import sandbox.java.sql.Timestamp;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("deprecation")
public class PreparedStatementWrapper extends StatementWrapper implements PreparedStatement {

    private final java.sql.PreparedStatement wrapped;

    public PreparedStatementWrapper(Connection connection, java.sql.PreparedStatement wrapped) {
        super(connection, wrapped);
        this.wrapped = wrapped;
    }

    public void addBatch() throws SQLException {
        try {
            wrapped.addBatch();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void clearParameters() throws SQLException {
        try {
            wrapped.clearParameters();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean execute() throws SQLException {
        try {
            return wrapped.execute();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet executeQuery() throws SQLException {
        try {
            return new ResultSetWrapper(wrapped.executeQuery());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int executeUpdate() throws SQLException {
        try {
            return wrapped.executeUpdate();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            return new ResultSetMetaDataWrapper(wrapped.getMetaData());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            return new ParameterMetaDataWrapper(wrapped.getParameterMetaData());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            wrapped.setArray(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            wrapped.setAsciiStream(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            wrapped.setAsciiStream(parameterIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            wrapped.setAsciiStream(parameterIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            wrapped.setBigDecimal(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            wrapped.setBinaryStream(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            wrapped.setBinaryStream(parameterIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            wrapped.setBinaryStream(parameterIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            wrapped.setBlob(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            wrapped.setBlob(parameterIndex, inputStream);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            wrapped.setBlob(parameterIndex, inputStream, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            wrapped.setBoolean(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            wrapped.setByte(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            wrapped.setBytes(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            wrapped.setCharacterStream(parameterIndex, reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        try {
            wrapped.setCharacterStream(parameterIndex, reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            wrapped.setCharacterStream(parameterIndex, reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            wrapped.setClob(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            wrapped.setClob(parameterIndex, reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            wrapped.setClob(parameterIndex, reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        try {
            wrapped.setDate(parameterIndex, x.toJsDate());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        try {
            wrapped.setDate(parameterIndex, x.toJsDate(), cal);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            wrapped.setDouble(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            wrapped.setFloat(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            wrapped.setInt(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setLargeMaxRows(long max) throws SQLException {
        try {
            wrapped.setLargeMaxRows(max);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            wrapped.setLong(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        try {
            wrapped.setNCharacterStream(parameterIndex, value);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        try {
            wrapped.setNCharacterStream(parameterIndex, value, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        try {
            wrapped.setNClob(parameterIndex, value);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            wrapped.setNClob(parameterIndex, reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            wrapped.setNClob(parameterIndex, reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        try {
            wrapped.setNString(parameterIndex, String.fromDJVM(value));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            wrapped.setNull(parameterIndex, sqlType);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            wrapped.setNull(parameterIndex, sqlType, String.fromDJVM(typeName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            wrapped.setObject(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        try {
            wrapped.setObject(parameterIndex, x, targetSqlType);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        try {
            wrapped.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try {
            wrapped.setRef(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        try {
            wrapped.setRowId(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            wrapped.setShort(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        try {
            wrapped.setSQLXML(parameterIndex, xmlObject);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            wrapped.setString(parameterIndex, String.fromDJVM(x));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        try {
            wrapped.setTime(parameterIndex, x.toJsTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        try {
            wrapped.setTime(parameterIndex, x.toJsTime(), cal);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        try {
            wrapped.setTimestamp(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        try {
            wrapped.setTimestamp(parameterIndex, x, cal);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            wrapped.setUnicodeStream(parameterIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        try {
            wrapped.setURL(parameterIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

}
