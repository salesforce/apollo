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
import java.util.Map;

import sandbox.java.lang.Object;
import sandbox.java.lang.String;
import sandbox.java.sql.Array;
import sandbox.java.sql.Blob;
import sandbox.java.sql.CallableStatement;
import sandbox.java.sql.Clob;
import sandbox.java.sql.Connection;
import sandbox.java.sql.Date;
import sandbox.java.sql.NClob;
import sandbox.java.sql.ParameterMetaData;
import sandbox.java.sql.Ref;
import sandbox.java.sql.ResultSet;
import sandbox.java.sql.ResultSetMetaData;
import sandbox.java.sql.RowId;
import sandbox.java.sql.SQLException;
import sandbox.java.sql.SQLType;
import sandbox.java.sql.SQLWarning;
import sandbox.java.sql.SQLXML;
import sandbox.java.sql.Time;
import sandbox.java.sql.Timestamp;

/**
 * @author hal.hildebrand
 *
 */
public class CallableStatementWrapper extends PreparedStatementWrapper implements CallableStatement {

    private final java.sql.CallableStatement wrapped;

    public CallableStatementWrapper(Connection connection, java.sql.CallableStatement wrapped) {
        super(connection, wrapped);
        this.wrapped = wrapped;
    }

    public Array getArray(int parameterIndex) throws SQLException {
        return wrapped.getArray(parameterIndex);
    }

    public Array getArray(String parameterName) throws SQLException {
        return wrapped.getArray(parameterName);
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return wrapped.getBigDecimal(parameterIndex);
    }

    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return wrapped.getBigDecimal(parameterIndex, scale);
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return wrapped.getBigDecimal(parameterName);
    }

    public Blob getBlob(int parameterIndex) throws SQLException {
        return wrapped.getBlob(parameterIndex);
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return wrapped.getBlob(parameterName);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        return wrapped.getBoolean(parameterIndex);
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return wrapped.getBoolean(parameterName);
    }

    public byte getByte(int parameterIndex) throws SQLException {
        return wrapped.getByte(parameterIndex);
    }

    public byte getByte(String parameterName) throws SQLException {
        return wrapped.getByte(parameterName);
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return wrapped.getBytes(parameterIndex);
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return wrapped.getBytes(parameterName);
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return wrapped.getCharacterStream(parameterIndex);
    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        return wrapped.getCharacterStream(parameterName);
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        return wrapped.getClob(parameterIndex);
    }

    public Clob getClob(String parameterName) throws SQLException {
        return wrapped.getClob(parameterName);
    }

    public Date getDate(int parameterIndex) throws SQLException {
        return wrapped.getDate(parameterIndex);
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return wrapped.getDate(parameterIndex, cal);
    }

    public Date getDate(String parameterName) throws SQLException {
        return wrapped.getDate(parameterName);
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return wrapped.getDate(parameterName, cal);
    }

    public double getDouble(int parameterIndex) throws SQLException {
        return wrapped.getDouble(parameterIndex);
    }

    public double getDouble(String parameterName) throws SQLException {
        return wrapped.getDouble(parameterName);
    }

    public float getFloat(int parameterIndex) throws SQLException {
        return wrapped.getFloat(parameterIndex);
    }

    public float getFloat(String parameterName) throws SQLException {
        return wrapped.getFloat(parameterName);
    }

    public int getInt(int parameterIndex) throws SQLException {
        return wrapped.getInt(parameterIndex);
    }

    public int getInt(String parameterName) throws SQLException {
        return wrapped.getInt(parameterName);
    }

    public long getLong(int parameterIndex) throws SQLException {
        return wrapped.getLong(parameterIndex);
    }

    public long getLong(String parameterName) throws SQLException {
        return wrapped.getLong(parameterName);
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return wrapped.getNCharacterStream(parameterIndex);
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return wrapped.getNCharacterStream(parameterName);
    }

    public NClob getNClob(int parameterIndex) throws SQLException {
        return wrapped.getNClob(parameterIndex);
    }

    public NClob getNClob(String parameterName) throws SQLException {
        return wrapped.getNClob(parameterName);
    }

    public String getNString(int parameterIndex) throws SQLException {
        return wrapped.getNString(parameterIndex);
    }

    public String getNString(String parameterName) throws SQLException {
        return wrapped.getNString(parameterName);
    }

    public Object getObject(int parameterIndex) throws SQLException {
        return wrapped.getObject(parameterIndex);
    }

    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return wrapped.getObject(parameterIndex, type);
    }

    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return wrapped.getObject(parameterIndex, map);
    }

    public Object getObject(String parameterName) throws SQLException {
        return wrapped.getObject(parameterName);
    }

    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return wrapped.getObject(parameterName, type);
    }

    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return wrapped.getObject(parameterName, map);
    }

    public Ref getRef(int parameterIndex) throws SQLException {
        return wrapped.getRef(parameterIndex);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return wrapped.getRef(parameterName);
    }

    public RowId getRowId(int parameterIndex) throws SQLException {
        return wrapped.getRowId(parameterIndex);
    }

    public RowId getRowId(String parameterName) throws SQLException {
        return wrapped.getRowId(parameterName);
    }

    public short getShort(int parameterIndex) throws SQLException {
        return wrapped.getShort(parameterIndex);
    }

    public short getShort(String parameterName) throws SQLException {
        return wrapped.getShort(parameterName);
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return wrapped.getSQLXML(parameterIndex);
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return wrapped.getSQLXML(parameterName);
    }

    public String getString(int parameterIndex) throws SQLException {
        return wrapped.getString(parameterIndex);
    }

    public String getString(String parameterName) throws SQLException {
        return wrapped.getString(parameterName);
    }

    public Time getTime(int parameterIndex) throws SQLException {
        return wrapped.getTime(parameterIndex);
    }

    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return wrapped.getTime(parameterIndex, cal);
    }

    public Time getTime(String parameterName) throws SQLException {
        return wrapped.getTime(parameterName);
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return wrapped.getTime(parameterName, cal);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return wrapped.getTimestamp(parameterIndex);
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return wrapped.getTimestamp(parameterIndex, cal);
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return wrapped.getTimestamp(parameterName);
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return wrapped.getTimestamp(parameterName, cal);
    }

    public URL getURL(int parameterIndex) throws SQLException {
        return wrapped.getURL(parameterIndex);
    }

    public URL getURL(String parameterName) throws SQLException {
        return wrapped.getURL(parameterName);
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        wrapped.registerOutParameter(parameterIndex, sqlType);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        wrapped.registerOutParameter(parameterIndex, sqlType, scale);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        wrapped.registerOutParameter(parameterIndex, sqlType, typeName);
    }

    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        wrapped.registerOutParameter(parameterName, sqlType);
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        wrapped.registerOutParameter(parameterName, sqlType, scale);
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        wrapped.registerOutParameter(parameterName, sqlType, typeName);
    }

    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        wrapped.setAsciiStream(parameterName, x);
    }

    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        wrapped.setAsciiStream(parameterName, x, length);
    }

    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        wrapped.setAsciiStream(parameterName, x, length);
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        wrapped.setBigDecimal(parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        wrapped.setBinaryStream(parameterName, x);
    }

    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        wrapped.setBinaryStream(parameterName, x, length);
    }

    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        wrapped.setBinaryStream(parameterName, x, length);
    }

    public void setBlob(String parameterName, Blob x) throws SQLException {
        wrapped.setBlob(parameterName, x);
    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        wrapped.setBlob(parameterName, inputStream);
    }

    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        wrapped.setBlob(parameterName, inputStream, length);
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException {
        wrapped.setBoolean(parameterName, x);
    }

    public void setByte(String parameterName, byte x) throws SQLException {
        wrapped.setByte(parameterName, x);
    }

    public void setBytes(String parameterName, byte[] x) throws SQLException {
        wrapped.setBytes(parameterName, x);
    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        wrapped.setCharacterStream(parameterName, reader);
    }

    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        wrapped.setCharacterStream(parameterName, reader, length);
    }

    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        wrapped.setCharacterStream(parameterName, reader, length);
    }

    public void setClob(String parameterName, Clob x) throws SQLException {
        wrapped.setClob(parameterName, x);
    }

    public void setClob(String parameterName, Reader reader) throws SQLException {
        wrapped.setClob(parameterName, reader);
    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        wrapped.setClob(parameterName, reader, length);
    }

    public void setDate(String parameterName, Date x) throws SQLException {
        wrapped.setDate(parameterName, x);
    }

    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        wrapped.setDate(parameterName, x, cal);
    }

    public void setDouble(String parameterName, double x) throws SQLException {
        wrapped.setDouble(parameterName, x);
    }

    public void setFloat(String parameterName, float x) throws SQLException {
        wrapped.setFloat(parameterName, x);
    }

    public void setInt(String parameterName, int x) throws SQLException {
        wrapped.setInt(parameterName, x);
    }

    public void setLong(String parameterName, long x) throws SQLException {
        wrapped.setLong(parameterName, x);
    }

    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        wrapped.setNCharacterStream(parameterName, value);
    }

    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        wrapped.setNCharacterStream(parameterName, value, length);
    }

    public void setNClob(String parameterName, NClob value) throws SQLException {
        wrapped.setNClob(parameterName, value);
    }

    public void setNClob(String parameterName, Reader reader) throws SQLException {
        wrapped.setNClob(parameterName, reader);
    }

    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        wrapped.setNClob(parameterName, reader, length);
    }

    public void setNString(String parameterName, String value) throws SQLException {
        wrapped.setNString(parameterName, value);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        wrapped.setNull(parameterName, sqlType);
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        wrapped.setNull(parameterName, sqlType, typeName);
    }

    public void setObject(String parameterName, Object x) throws SQLException {
        wrapped.setObject(parameterName, x);
    }

    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        wrapped.setObject(parameterName, x, targetSqlType);
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        wrapped.setObject(parameterName, x, targetSqlType, scale);
    }

    public void setRowId(String parameterName, RowId x) throws SQLException {
        wrapped.setRowId(parameterName, x);
    }

    public void setShort(String parameterName, short x) throws SQLException {
        wrapped.setShort(parameterName, x);
    }

    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        wrapped.setSQLXML(parameterName, xmlObject);
    }

    public void setString(String parameterName, String x) throws SQLException {
        wrapped.setString(parameterName, x);
    }

    public void setTime(String parameterName, Time x) throws SQLException {
        wrapped.setTime(parameterName, x);
    }

    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        wrapped.setTime(parameterName, x, cal);
    }

    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        wrapped.setTimestamp(parameterName, x);
    }

    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        wrapped.setTimestamp(parameterName, x, cal);
    }

    public void setURL(String parameterName, URL val) throws SQLException {
        wrapped.setURL(parameterName, val);
    }

    public boolean wasNull() throws SQLException {
        return wrapped.wasNull();
    }

}
