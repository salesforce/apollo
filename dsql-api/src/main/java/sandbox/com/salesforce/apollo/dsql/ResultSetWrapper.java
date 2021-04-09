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
import sandbox.java.lang.String;
import sandbox.java.sql.Array;
import sandbox.java.sql.Blob;
import sandbox.java.sql.Clob;
import sandbox.java.sql.Date;
import sandbox.java.sql.NClob;
import sandbox.java.sql.Ref;
import sandbox.java.sql.ResultSet;
import sandbox.java.sql.ResultSetMetaData;
import sandbox.java.sql.RowId;
import sandbox.java.sql.SQLException;
import sandbox.java.sql.SQLType;
import sandbox.java.sql.SQLWarning;
import sandbox.java.sql.SQLXML;
import sandbox.java.sql.Statement;
import sandbox.java.sql.Time;
import sandbox.java.sql.Timestamp;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("deprecation")
public class ResultSetWrapper implements ResultSet {
    private final java.sql.ResultSet wrapped;

    public ResultSetWrapper(java.sql.ResultSet wrapped) {
        this.wrapped = wrapped;
    }

    public boolean absolute(int row) throws SQLException {
        try {
            return wrapped.absolute(row);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void afterLast() throws SQLException {
        try {
            wrapped.afterLast();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void beforeFirst() throws SQLException {
        try {
            wrapped.beforeFirst();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void cancelRowUpdates() throws SQLException {
        try {
            wrapped.cancelRowUpdates();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void clearWarnings() throws SQLException {
        try {
            wrapped.clearWarnings();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void close() throws SQLException {
        try {
            wrapped.close();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void deleteRow() throws SQLException {
        try {
            wrapped.deleteRow();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int findColumn(String columnLabel) throws SQLException {
        try {
            return wrapped.findColumn(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean first() throws SQLException {
        try {
            return wrapped.first();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Array getArray(int columnIndex) throws SQLException {
        try {
            return new ArrayWrapper(wrapped.getArray(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Array getArray(String columnLabel) throws SQLException {
        try {
            return new ArrayWrapper(wrapped.getArray(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        try {
            return wrapped.getAsciiStream(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        try {
            return wrapped.getAsciiStream(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        try {
            return wrapped.getBigDecimal(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        try {
            return wrapped.getBigDecimal(columnIndex, scale);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        try {
            return wrapped.getBigDecimal(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        try {
            return wrapped.getBigDecimal(String.fromDJVM(columnLabel), scale);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        try {
            return wrapped.getBinaryStream(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        try {
            return wrapped.getBinaryStream(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        try {
            return new BlobWrapper(wrapped.getBlob(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        try {
            return new BlobWrapper(wrapped.getBlob(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        try {
            return wrapped.getBoolean(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        try {
            return wrapped.getBoolean(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte getByte(int columnIndex) throws SQLException {
        try {
            return wrapped.getByte(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte getByte(String columnLabel) throws SQLException {
        try {
            return wrapped.getByte(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            return wrapped.getBytes(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        try {
            return wrapped.getBytes(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        try {
            return wrapped.getCharacterStream(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        try {
            return wrapped.getCharacterStream(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Clob getClob(int columnIndex) throws SQLException {
        try {
            return new ClobWrapper(wrapped.getClob(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Clob getClob(String columnLabel) throws SQLException {
        try {
            return new ClobWrapper(wrapped.getClob(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getConcurrency() throws SQLException {
        try {
            return wrapped.getConcurrency();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getCursorName() throws SQLException {
        try {
            return String.toDJVM(wrapped.getCursorName());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Date getDate(int columnIndex) throws SQLException {
        try {
            return new Date(wrapped.getDate(columnIndex).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        try {
            return new Date(wrapped.getDate(columnIndex, cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Date getDate(String columnLabel) throws SQLException {
        try {
            return new Date(wrapped.getDate(String.fromDJVM(columnLabel)).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        try {
            return new Date(wrapped.getDate(String.fromDJVM(columnLabel), cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public double getDouble(int columnIndex) throws SQLException {
        try {
            return wrapped.getDouble(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public double getDouble(String columnLabel) throws SQLException {
        try {
            return wrapped.getDouble(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getFetchDirection() throws SQLException {
        try {
            return wrapped.getFetchDirection();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getFetchSize() throws SQLException {
        try {
            return wrapped.getFetchSize();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public float getFloat(int columnIndex) throws SQLException {
        try {
            return wrapped.getFloat(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public float getFloat(String columnLabel) throws SQLException {
        try {
            return wrapped.getFloat(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getHoldability() throws SQLException {
        try {
            return wrapped.getHoldability();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getInt(int columnIndex) throws SQLException {
        try {
            return wrapped.getInt(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getInt(String columnLabel) throws SQLException {
        try {
            return wrapped.getInt(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long getLong(int columnIndex) throws SQLException {
        try {
            return wrapped.getLong(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public long getLong(String columnLabel) throws SQLException {
        try {
            return wrapped.getLong(String.fromDJVM(columnLabel));
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

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        try {
            return wrapped.getNCharacterStream(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        try {
            return wrapped.getNCharacterStream(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        try {
            return new NClobWrapper(wrapped.getNClob(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        try {
            return new NClobWrapper(wrapped.getNClob(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getNString(int columnIndex) throws SQLException {
        try {
            return String.toDJVM(wrapped.getNString(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getNString(String columnLabel) throws SQLException {
        try {
            return String.toDJVM(wrapped.getNString(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getObject(int columnIndex) throws SQLException {
        try {
            return DJVM.sandbox(wrapped.getObject(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        try {
            return wrapped.getObject(columnIndex, type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        try {
            return wrapped.getObject(columnIndex, convertClassMap(map));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getObject(String columnLabel) throws SQLException {
        try {
            return wrapped.getObject(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        try {
            return wrapped.getObject(String.fromDJVM(columnLabel), type);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        try {
            return wrapped.getObject(String.fromDJVM(columnLabel), convertClassMap(map));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Ref getRef(int columnIndex) throws SQLException {
        try {
            return new RefWrapper(wrapped.getRef(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Ref getRef(String columnLabel) throws SQLException {
        try {
            return new RefWrapper(wrapped.getRef(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getRow() throws SQLException {
        try {
            return wrapped.getRow();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        try {
            return new RowIdWrapper(wrapped.getRowId(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        try {
            return new RowIdWrapper(wrapped.getRowId(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public short getShort(int columnIndex) throws SQLException {
        try {
            return wrapped.getShort(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public short getShort(String columnLabel) throws SQLException {
        try {
            return wrapped.getShort(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        try {
            return new SQLXMLWrapper(wrapped.getSQLXML(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        try {
            return new SQLXMLWrapper(wrapped.getSQLXML(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Statement getStatement() throws SQLException {
        try {
            java.sql.Statement statement = wrapped.getStatement();
            return new StatementWrapper(new ConnectionWrapper(statement.getConnection()), statement);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getString(int columnIndex) throws SQLException {
        try {
            return String.toDJVM(wrapped.getString(columnIndex));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getString(String columnLabel) throws SQLException {
        try {
            return String.toDJVM(wrapped.getString(String.fromDJVM(columnLabel)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Time getTime(int columnIndex) throws SQLException {
        try {
            return new Time(wrapped.getTime(columnIndex).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        try {
            return new Time(wrapped.getTime(columnIndex, cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Time getTime(String columnLabel) throws SQLException {
        try {
            return new Time(wrapped.getTime(String.fromDJVM(columnLabel)).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        try {
            return new Time(wrapped.getTime(String.fromDJVM(columnLabel), cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try {
            return new Timestamp(wrapped.getTimestamp(columnIndex).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        try {
            return new Timestamp(wrapped.getTimestamp(columnIndex, cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        try {
            return new Timestamp(wrapped.getTimestamp(String.fromDJVM(columnLabel)).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        try {
            return new Timestamp(wrapped.getTimestamp(String.fromDJVM(columnLabel), cal).getTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getType() throws SQLException {
        try {
            return wrapped.getType();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        try {
            return wrapped.getUnicodeStream(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        try {
            return wrapped.getUnicodeStream(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public URL getURL(int columnIndex) throws SQLException {
        try {
            return wrapped.getURL(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public URL getURL(String columnLabel) throws SQLException {
        try {
            return wrapped.getURL(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        try {
            java.sql.SQLWarning warnings = wrapped.getWarnings();
            return new SQLWarning(String.toDJVM(warnings.getMessage()), String.toDJVM(warnings.getSQLState()),
                    warnings.getErrorCode());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void insertRow() throws SQLException {
        try {
            wrapped.insertRow();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isAfterLast() throws SQLException {
        try {
            return wrapped.isAfterLast();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isBeforeFirst() throws SQLException {
        try {
            return wrapped.isBeforeFirst();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isClosed() throws SQLException {
        try {
            return wrapped.isClosed();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isFirst() throws SQLException {
        try {
            return wrapped.isFirst();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isLast() throws SQLException {
        try {
            return wrapped.isLast();
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

    public boolean last() throws SQLException {
        try {
            return wrapped.last();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void moveToCurrentRow() throws SQLException {
        try {
            wrapped.moveToCurrentRow();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void moveToInsertRow() throws SQLException {
        try {
            wrapped.moveToInsertRow();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean next() throws SQLException {
        try {
            return wrapped.next();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean previous() throws SQLException {
        try {
            return wrapped.previous();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void refreshRow() throws SQLException {
        try {
            wrapped.refreshRow();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean relative(int rows) throws SQLException {
        try {
            return wrapped.relative(rows);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean rowDeleted() throws SQLException {
        try {
            return wrapped.rowDeleted();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean rowInserted() throws SQLException {
        try {
            return wrapped.rowInserted();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean rowUpdated() throws SQLException {
        try {
            return wrapped.rowUpdated();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setFetchDirection(int direction) throws SQLException {
        try {
            wrapped.setFetchDirection(direction);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setFetchSize(int rows) throws SQLException {
        try {
            wrapped.setFetchSize(rows);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public java.sql.ResultSet toJsResultSet() {
        return wrapped;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return wrapped.unwrap(iface);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        try {
            wrapped.updateArray(columnIndex, x.toJsArray());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        try {
            wrapped.updateArray(String.fromDJVM(columnLabel), x.toJsArray());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        try {
            wrapped.updateAsciiStream(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        try {
            wrapped.updateAsciiStream(columnIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            wrapped.updateAsciiStream(columnIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        try {
            wrapped.updateAsciiStream(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        try {
            wrapped.updateAsciiStream(String.fromDJVM(columnLabel), x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        try {
            wrapped.updateAsciiStream(String.fromDJVM(columnLabel), x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        try {
            wrapped.updateBigDecimal(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        try {
            wrapped.updateBigDecimal(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        try {
            wrapped.updateBinaryStream(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        try {
            wrapped.updateBinaryStream(columnIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            wrapped.updateBinaryStream(columnIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        try {
            wrapped.updateBinaryStream(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        try {
            wrapped.updateBinaryStream(String.fromDJVM(columnLabel), x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        try {
            wrapped.updateBinaryStream(String.fromDJVM(columnLabel), x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        try {
            wrapped.updateBlob(columnIndex, x.toJsBlob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        try {
            wrapped.updateBlob(columnIndex, inputStream);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        try {
            wrapped.updateBlob(columnIndex, inputStream, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        try {
            wrapped.updateBlob(String.fromDJVM(columnLabel), x.toJsBlob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        try {
            wrapped.updateBlob(String.fromDJVM(columnLabel), inputStream);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        try {
            wrapped.updateBlob(String.fromDJVM(columnLabel), inputStream, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        try {
            wrapped.updateBoolean(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        try {
            wrapped.updateBoolean(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        try {
            wrapped.updateByte(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        try {
            wrapped.updateByte(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        try {
            wrapped.updateBytes(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        try {
            wrapped.updateBytes(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        try {
            wrapped.updateCharacterStream(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        try {
            wrapped.updateCharacterStream(columnIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        try {
            wrapped.updateCharacterStream(columnIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        try {
            wrapped.updateCharacterStream(String.fromDJVM(columnLabel), reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        try {
            wrapped.updateCharacterStream(String.fromDJVM(columnLabel), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        try {
            wrapped.updateCharacterStream(String.fromDJVM(columnLabel), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        try {
            wrapped.updateClob(columnIndex, x.toJsClob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        try {
            wrapped.updateClob(columnIndex, reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        try {
            wrapped.updateClob(columnIndex, reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        try {
            wrapped.updateClob(String.fromDJVM(columnLabel), x.toJsClob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        try {
            wrapped.updateClob(String.fromDJVM(columnLabel), reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        try {
            wrapped.updateClob(String.fromDJVM(columnLabel), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        try {
            wrapped.updateDate(columnIndex, new java.sql.Date(x.getTime()));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        try {
            wrapped.updateDate(String.fromDJVM(columnLabel), new java.sql.Date(x.getTime()));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        try {
            wrapped.updateDouble(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        try {
            wrapped.updateDouble(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        try {
            wrapped.updateFloat(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        try {
            wrapped.updateFloat(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        try {
            wrapped.updateInt(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        try {
            wrapped.updateInt(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        try {
            wrapped.updateLong(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        try {
            wrapped.updateLong(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        try {
            wrapped.updateNCharacterStream(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        try {
            wrapped.updateNCharacterStream(columnIndex, x, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        try {
            wrapped.updateNCharacterStream(String.fromDJVM(columnLabel), reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        try {
            wrapped.updateNCharacterStream(String.fromDJVM(columnLabel), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        try {
            wrapped.updateNClob(columnIndex, nClob.toJsNClob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        try {
            wrapped.updateNClob(columnIndex, reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        try {
            wrapped.updateNClob(columnIndex, reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        try {
            wrapped.updateNClob(String.fromDJVM(columnLabel), nClob.toJsNClob());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        try {
            wrapped.updateNClob(String.fromDJVM(columnLabel), reader);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        try {
            wrapped.updateNClob(String.fromDJVM(columnLabel), reader, length);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        try {
            wrapped.updateNString(columnIndex, String.fromDJVM(nString));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        try {
            wrapped.updateNString(String.fromDJVM(columnLabel), String.fromDJVM(nString));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNull(int columnIndex) throws SQLException {
        try {
            wrapped.updateNull(columnIndex);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateNull(String columnLabel) throws SQLException {
        try {
            wrapped.updateNull(String.fromDJVM(columnLabel));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        try {
            wrapped.updateObject(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        try {
            wrapped.updateObject(columnIndex, x, scaleOrLength);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        try {
            wrapped.updateObject(columnIndex, x, targetSqlType.toJsSQLType());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        try {
            wrapped.updateObject(columnIndex, x, targetSqlType.toJsSQLType(), scaleOrLength);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        try {
            wrapped.updateObject(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        try {
            wrapped.updateObject(String.fromDJVM(columnLabel), x, scaleOrLength);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        try {
            wrapped.updateObject(String.fromDJVM(columnLabel), x, targetSqlType.toJsSQLType());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateObject(String columnLabel, Object x, SQLType targetSqlType,
                             int scaleOrLength) throws SQLException {
        try {
            wrapped.updateObject(String.fromDJVM(columnLabel), x, targetSqlType.toJsSQLType(), scaleOrLength);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        try {
            wrapped.updateRef(columnIndex, x.toJsRef());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        try {
            wrapped.updateRef(String.fromDJVM(columnLabel), x.toJsRef());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateRow() throws SQLException {
        try {
            wrapped.updateRow();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        try {
            wrapped.updateRowId(columnIndex, x.toJsRowId());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        try {
            wrapped.updateRowId(String.fromDJVM(columnLabel), x.toJsRowId());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        try {
            wrapped.updateShort(columnIndex, x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        try {
            wrapped.updateShort(String.fromDJVM(columnLabel), x);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        try {
            wrapped.updateSQLXML(columnIndex, xmlObject.toJsSQLXML());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        try {
            wrapped.updateSQLXML(String.fromDJVM(columnLabel), xmlObject.toJsSQLXML());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        try {
            wrapped.updateString(columnIndex, String.fromDJVM(x));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        try {
            wrapped.updateString(String.fromDJVM(columnLabel), String.fromDJVM(x));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        try {
            wrapped.updateTime(columnIndex, x.toJsTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        try {
            wrapped.updateTime(String.fromDJVM(columnLabel), x.toJsTime());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        try {
            wrapped.updateTimestamp(columnIndex, x.toJsTimestamp());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        try {
            wrapped.updateTimestamp(String.fromDJVM(columnLabel), x.toJsTimestamp());
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
