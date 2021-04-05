/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import static sandbox.com.salesforce.apollo.dsql.ConnectionWrapper.convertClassMap;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Calendar;
import java.util.Map;

import sandbox.java.lang.DJVM;
import sandbox.java.lang.Object;
import sandbox.java.lang.String;
import sandbox.java.sql.Array;
import sandbox.java.sql.Blob;
import sandbox.java.sql.CallableStatement;
import sandbox.java.sql.Clob;
import sandbox.java.sql.Connection;
import sandbox.java.sql.Date;
import sandbox.java.sql.NClob;
import sandbox.java.sql.Ref;
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
public class CallableStatementWrapper extends PreparedStatementWrapper implements CallableStatement {

    private final java.sql.CallableStatement wrapped;

    public CallableStatementWrapper(Connection connection, java.sql.CallableStatement wrapped) {
        super(connection, wrapped);
        this.wrapped = wrapped;
    }

    public Array getArray(int parameterIndex) throws SQLException {
        try {
            return new ArrayWrapper(wrapped.getArray(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Array getArray(String parameterName) throws SQLException {
        try {
            return new ArrayWrapper(wrapped.getArray(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        try {
            return wrapped.getBigDecimal(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        try {
            return wrapped.getBigDecimal(parameterIndex, scale);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        try {
            return wrapped.getBigDecimal(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Blob getBlob(int parameterIndex) throws SQLException {
        try {
            return new BlobWrapper(wrapped.getBlob(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Blob getBlob(String parameterName) throws SQLException {
        try {
            return new BlobWrapper(wrapped.getBlob(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        try {
            return wrapped.getBoolean(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        try {
            return wrapped.getBoolean(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte getByte(int parameterIndex) throws SQLException {
        try {
            return wrapped.getByte(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte getByte(String parameterName) throws SQLException {
        try {
            return wrapped.getByte(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        try {
            return wrapped.getBytes(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        try {
            return wrapped.getBytes(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        try {
            return wrapped.getCharacterStream(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Reader getCharacterStream(String parameterName) throws SQLException {
        try {
            return wrapped.getCharacterStream(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Clob getClob(int parameterIndex) throws SQLException {
        try {
            return new ClobWrapper(wrapped.getClob(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Clob getClob(String parameterName) throws SQLException {
        try {
            return new ClobWrapper(wrapped.getClob(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Date getDate(int parameterIndex) throws SQLException {
        try {
            return new Date(wrapped.getDate(parameterIndex).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return new Date(wrapped.getDate(parameterIndex, cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Date getDate(String parameterName) throws SQLException {
        try {
            return new Date(wrapped.getDate(String.fromDJVM(parameterName)).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        try {
            return new Date(wrapped.getDate(String.fromDJVM(parameterName), cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public double getDouble(int parameterIndex) throws SQLException {
        try {
            return wrapped.getDouble(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public double getDouble(String parameterName) throws SQLException {
        try {
            return wrapped.getDouble(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public float getFloat(int parameterIndex) throws SQLException {
        try {
            return wrapped.getFloat(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public float getFloat(String parameterName) throws SQLException {
        try {
            return wrapped.getFloat(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getInt(int parameterIndex) throws SQLException {
        try {
            return wrapped.getInt(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getInt(String parameterName) throws SQLException {
        try {
            return wrapped.getInt(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long getLong(int parameterIndex) throws SQLException {
        try {
            return wrapped.getLong(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long getLong(String parameterName) throws SQLException {
        try {
            return wrapped.getLong(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        try {
            return wrapped.getNCharacterStream(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Reader getNCharacterStream(String parameterName) throws SQLException {
        try {
            return wrapped.getNCharacterStream(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public NClob getNClob(int parameterIndex) throws SQLException {
        try {
            return new NClobWrapper(wrapped.getNClob(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public NClob getNClob(String parameterName) throws SQLException {
        try {
            return new NClobWrapper(wrapped.getNClob(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getNString(int parameterIndex) throws SQLException {
        try {
            return String.toDJVM(wrapped.getNString(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getNString(String parameterName) throws SQLException {
        try {
            return String.toDJVM(wrapped.getNString(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getObject(int parameterIndex) throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getObject(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        try {
            return wrapped.getObject(parameterIndex, type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getObject(parameterIndex, convertClassMap(map)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public Object getObject(String parameterName) throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getObject(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        try {
            return (T) DJVM.sandbox(wrapped.getObject(String.fromDJVM(parameterName), type));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getObject(String.fromDJVM(parameterName), convertClassMap(map)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public Ref getRef(int parameterIndex) throws SQLException {
        try {
            return new RefWrapper(wrapped.getRef(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Ref getRef(String parameterName) throws SQLException {
        try {
            return new RefWrapper(wrapped.getRef(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public RowId getRowId(int parameterIndex) throws SQLException {
        try {
            return new RowIdWrapper(wrapped.getRowId(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public RowId getRowId(String parameterName) throws SQLException {
        try {
            return new RowIdWrapper(wrapped.getRowId(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public short getShort(int parameterIndex) throws SQLException {
        try {
            return wrapped.getShort(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public short getShort(String parameterName) throws SQLException {
        try {
            return wrapped.getShort(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        try {
            return new SQLXMLWrapper(wrapped.getSQLXML(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public SQLXML getSQLXML(String parameterName) throws SQLException {
        try {
            return new SQLXMLWrapper(wrapped.getSQLXML(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getString(int parameterIndex) throws SQLException {
        try {
            return String.toDJVM(wrapped.getString(parameterIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getString(String parameterName) throws SQLException {
        try {
            return String.toDJVM(wrapped.getString(String.fromDJVM(parameterName)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Time getTime(int parameterIndex) throws SQLException {
        try {
            return new Time(wrapped.getTime(parameterIndex).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return new Time(wrapped.getTime(parameterIndex, cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Time getTime(String parameterName) throws SQLException {
        try {
            return new Time(wrapped.getTime(String.fromDJVM(parameterName)).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        try {
            return new Time(wrapped.getTime(String.fromDJVM(parameterName), cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        try {
            return new Timestamp(wrapped.getTimestamp(parameterIndex).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return new Timestamp(wrapped.getTimestamp(parameterIndex, cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Timestamp getTimestamp(String parameterName) throws SQLException {
        try {
            return new Timestamp(wrapped.getTimestamp(String.fromDJVM(parameterName)).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        try {
            return new Timestamp(wrapped.getTimestamp(String.fromDJVM(parameterName), cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public URL getURL(int parameterIndex) throws SQLException {
        try {
            return wrapped.getURL(parameterIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public URL getURL(String parameterName) throws SQLException {
        try {
            return wrapped.getURL(String.fromDJVM(parameterName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        try {
            wrapped.registerOutParameter(parameterIndex, sqlType);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        try {
            wrapped.registerOutParameter(parameterIndex, sqlType, scale);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            wrapped.registerOutParameter(parameterIndex, sqlType, String.fromDJVM(typeName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        try {
            wrapped.registerOutParameter(String.fromDJVM(parameterName), sqlType);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        try {
            wrapped.registerOutParameter(String.fromDJVM(parameterName), sqlType, scale);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        try {
            wrapped.registerOutParameter(String.fromDJVM(parameterName), sqlType, String.fromDJVM(typeName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        try {
            wrapped.setAsciiStream(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            wrapped.setAsciiStream(String.fromDJVM(parameterName), x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            wrapped.setAsciiStream(String.fromDJVM(parameterName), x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        try {
            wrapped.setBigDecimal(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        try {
            wrapped.setBinaryStream(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            wrapped.setBinaryStream(String.fromDJVM(parameterName), x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            wrapped.setBinaryStream(String.fromDJVM(parameterName), x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBlob(String parameterName, Blob x) throws SQLException {
        try {
            wrapped.setBlob(String.fromDJVM(parameterName), x.toJsBlob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        try {
            wrapped.setBlob(String.fromDJVM(parameterName), inputStream);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        try {
            wrapped.setBlob(String.fromDJVM(parameterName), inputStream, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBoolean(String parameterName, boolean x) throws SQLException {
        try {
            wrapped.setBoolean(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setByte(String parameterName, byte x) throws SQLException {
        try {
            wrapped.setByte(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setBytes(String parameterName, byte[] x) throws SQLException {
        try {
            wrapped.setBytes(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        try {
            wrapped.setCharacterStream(String.fromDJVM(parameterName), reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        try {
            wrapped.setCharacterStream(String.fromDJVM(parameterName), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        try {
            wrapped.setCharacterStream(String.fromDJVM(parameterName), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setClob(String parameterName, Clob x) throws SQLException {
        try {
            wrapped.setClob(String.fromDJVM(parameterName), x.toJsClob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setClob(String parameterName, Reader reader) throws SQLException {
        try {
            wrapped.setClob(String.fromDJVM(parameterName), reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            wrapped.setClob(String.fromDJVM(parameterName), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setDate(String parameterName, Date x) throws SQLException {
        try {
            wrapped.setDate(String.fromDJVM(parameterName), x.toJsDate());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        try {
            wrapped.setDate(String.fromDJVM(parameterName), x.toJsDate(), cal);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setDouble(String parameterName, double x) throws SQLException {
        try {
            wrapped.setDouble(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setFloat(String parameterName, float x) throws SQLException {
        try {
            wrapped.setFloat(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setInt(String parameterName, int x) throws SQLException {
        try {
            wrapped.setInt(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setLong(String parameterName, long x) throws SQLException {
        try {
            wrapped.setLong(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        try {
            wrapped.setNCharacterStream(String.fromDJVM(parameterName), value);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        try {
            wrapped.setNCharacterStream(String.fromDJVM(parameterName), value, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNClob(String parameterName, NClob value) throws SQLException {
        try {
            wrapped.setNClob(String.fromDJVM(parameterName), value.toJsNClob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNClob(String parameterName, Reader reader) throws SQLException {
        try {
            wrapped.setNClob(String.fromDJVM(parameterName), reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            wrapped.setNClob(String.fromDJVM(parameterName), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNString(String parameterName, String value) throws SQLException {
        try {
            wrapped.setNString(String.fromDJVM(parameterName), String.fromDJVM(value));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        try {
            wrapped.setNull(String.fromDJVM(parameterName), sqlType);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        try {
            wrapped.setNull(String.fromDJVM(parameterName), sqlType, String.fromDJVM(typeName));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setObject(String parameterName, Object x) throws SQLException {
        try {
            wrapped.setObject(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        try {
            wrapped.setObject(String.fromDJVM(parameterName), x, targetSqlType);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        try {
            wrapped.setObject(String.fromDJVM(parameterName), x, targetSqlType, scale);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setRowId(String parameterName, RowId x) throws SQLException {
        try {
            wrapped.setRowId(String.fromDJVM(parameterName), x.toJsRowId());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setShort(String parameterName, short x) throws SQLException {
        try {
            wrapped.setShort(String.fromDJVM(parameterName), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        try {
            wrapped.setSQLXML(String.fromDJVM(parameterName), xmlObject.toJsSQLXML());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setString(String parameterName, String x) throws SQLException {
        try {
            wrapped.setString(String.fromDJVM(parameterName), String.fromDJVM(x));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTime(String parameterName, Time x) throws SQLException {
        try {
            wrapped.setTime(String.fromDJVM(parameterName), x.toJsTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        try {
            wrapped.setTime(String.fromDJVM(parameterName), x.toJsTime(), cal);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        try {
            wrapped.setTimestamp(String.fromDJVM(parameterName), x.toJsTimestamp());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        try {
            wrapped.setTimestamp(String.fromDJVM(parameterName), x.toJsTimestamp(), cal);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setURL(String parameterName, URL val) throws SQLException {
        try {
            wrapped.setURL(String.fromDJVM(parameterName), val);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean wasNull() throws SQLException {
        try {
            return wrapped.wasNull();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

}
