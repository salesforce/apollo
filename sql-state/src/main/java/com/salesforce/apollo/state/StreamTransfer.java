/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.salesforce.apollo.state;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.store.Data;
import org.h2.store.DataReader;
import org.h2.util.Bits;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.ExtTypeInfo;
import org.h2.value.ExtTypeInfoEnum;
import org.h2.value.ExtTypeInfoGeometry;
import org.h2.value.ExtTypeInfoNumeric;
import org.h2.value.ExtTypeInfoRow;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBinary;
import org.h2.value.ValueBlob;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueChar;
import org.h2.value.ValueClob;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueDouble;
import org.h2.value.ValueEnumBase;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInteger;
import org.h2.value.ValueInterval;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueRow;
import org.h2.value.ValueSmallint;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueTinyint;
import org.h2.value.ValueUuid;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;
import org.h2.value.ValueVarcharIgnoreCase;
import org.h2.value.lob.LobData;
import org.h2.value.lob.LobDataDatabase;
import org.h2.value.lob.LobDataFetchOnDemand;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * The stream transfer class is used to read and write Value objects to streams
 */
public class StreamTransfer {

    private static final int ARRAY  = 17;
    private static final int BIGINT = 5;
    // 2.0.202
    private static final int BINARY   = 30;
    private static final int BLOB     = 15;
    private static final int BOOLEAN  = 1;
    private static final int CHAR     = 21;
    private static final int CLOB     = 16;
    private static final int DATE     = 10;
    private static final int DECFLOAT = 31;
    private static final int DOUBLE   = 7;
    // 1.4.195
    private static final int ENUM     = 25;
    private static final int GEOMETRY = 22;
    private static final int INTEGER  = 4;
    // 1.4.198
    private static final int INTERVAL    = 26;
    private static final int JAVA_OBJECT = 19;
    // 1.4.200
    private static final int   JSON                = 28;
    private static final int   LOB_MAC_SALT_LENGTH = 16;
    private static final int   LOB_MAGIC           = 0x1234;
    private static final int   NULL                = 0;
    private static final int   NUMERIC             = 6;
    private static final int   REAL                = 8;
    private static final int   ROW                 = 27;
    private static final int   SMALLINT            = 3;
    private static final int[] TI_TO_VALUE         = new int[45];
    private static final int   TIME                = 9;
    private static final int   TIME_TZ             = 29;
    private static final int   TIMESTAMP           = 11;
    // 1.4.192
    private static final int   TIMESTAMP_TZ = 24;
    private static final int   TINYINT      = 2;
    private static final int   UUID         = 20;
    private static final int[] VALUE_TO_TI  = new int[Value.TYPE_COUNT + 1];
    private static final int   VARBINARY    = 12;

    private static final int VARCHAR            = 13;
    private static final int VARCHAR_IGNORECASE = 14;

    static {
        addType(-1, Value.UNKNOWN);
        addType(NULL, Value.NULL);
        addType(BOOLEAN, Value.BOOLEAN);
        addType(TINYINT, Value.TINYINT);
        addType(SMALLINT, Value.SMALLINT);
        addType(INTEGER, Value.INTEGER);
        addType(BIGINT, Value.BIGINT);
        addType(NUMERIC, Value.NUMERIC);
        addType(DOUBLE, Value.DOUBLE);
        addType(REAL, Value.REAL);
        addType(TIME, Value.TIME);
        addType(DATE, Value.DATE);
        addType(TIMESTAMP, Value.TIMESTAMP);
        addType(VARBINARY, Value.VARBINARY);
        addType(VARCHAR, Value.VARCHAR);
        addType(VARCHAR_IGNORECASE, Value.VARCHAR_IGNORECASE);
        addType(BLOB, Value.BLOB);
        addType(CLOB, Value.CLOB);
        addType(ARRAY, Value.ARRAY);
        addType(JAVA_OBJECT, Value.JAVA_OBJECT);
        addType(UUID, Value.UUID);
        addType(CHAR, Value.CHAR);
        addType(GEOMETRY, Value.GEOMETRY);
        addType(TIMESTAMP_TZ, Value.TIMESTAMP_TZ);
        addType(ENUM, Value.ENUM);
        addType(26, Value.INTERVAL_YEAR);
        addType(27, Value.INTERVAL_MONTH);
        addType(28, Value.INTERVAL_DAY);
        addType(29, Value.INTERVAL_HOUR);
        addType(30, Value.INTERVAL_MINUTE);
        addType(31, Value.INTERVAL_SECOND);
        addType(32, Value.INTERVAL_YEAR_TO_MONTH);
        addType(33, Value.INTERVAL_DAY_TO_HOUR);
        addType(34, Value.INTERVAL_DAY_TO_MINUTE);
        addType(35, Value.INTERVAL_DAY_TO_SECOND);
        addType(36, Value.INTERVAL_HOUR_TO_MINUTE);
        addType(37, Value.INTERVAL_HOUR_TO_SECOND);
        addType(38, Value.INTERVAL_MINUTE_TO_SECOND);
        addType(39, Value.ROW);
        addType(40, Value.JSON);
        addType(41, Value.TIME_TZ);
        addType(42, Value.BINARY);
        addType(43, Value.DECFLOAT);
    }

    private static void addType(int typeInformationType, int valueType) {
        VALUE_TO_TI[valueType + 1] = typeInformationType;
        TI_TO_VALUE[typeInformationType + 1] = valueType;
    }

    private final byte[]  lobMacSalt;
    private final Session session;
    private final int     version;

    public StreamTransfer(int version, Session session) {
        this.version = version;
        this.session = session;
        lobMacSalt = new byte[LOB_MAC_SALT_LENGTH];
        com.salesforce.apollo.utils.Utils.bitStreamEntropy().nextBytes(lobMacSalt);
    }

    public StreamTransfer(Session session) {
        this(Constants.TCP_PROTOCOL_VERSION_20, session);
    }

    public int getVersion() {
        return version;
    }

    public Value[] read(ByteString bs) {
        try (var in = new DataInputStream(BbBackedInputStream.aggregate(bs.asReadOnlyByteBufferList()))) {
            int len = readInt(in);
            return readArrayElements(len, null, in);
        } catch (IOException e) {
            throw new IllegalStateException("Should never happen", e);
        }
    }

    /**
     * Read a value.
     *
     * @param columnType the data type of value, or {@code null}
     * @return the value
     * @throws IOException on failure
     */
    public Value readValue(TypeInfo columnType, DataInputStream in) throws IOException {
        int type = readInt(in);
        switch (type) {
        case NULL:
            return ValueNull.INSTANCE;
        case VARBINARY:
            return ValueVarbinary.getNoCopy(readBytes(in));
        case BINARY:
            return ValueBinary.getNoCopy(readBytes(in));
        case UUID:
            return ValueUuid.get(readLong(in), readLong(in));
        case JAVA_OBJECT:
            return ValueJavaObject.getNoCopy(readBytes(in));
        case BOOLEAN:
            return ValueBoolean.get(readBoolean(in));
        case TINYINT:
            return ValueTinyint.get(readByte(in));
        case DATE:
            return ValueDate.fromDateValue(readLong(in));
        case TIME:
            return ValueTime.fromNanos(readLong(in));
        case TIME_TZ:
            return ValueTimeTimeZone.fromNanos(readLong(in), readInt(in));
        case TIMESTAMP:
            return ValueTimestamp.fromDateValueAndNanos(readLong(in), readLong(in));
        case TIMESTAMP_TZ: {
            long dateValue = readLong(in), timeNanos = readLong(in);
            int timeZoneOffset = readInt(in);
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
                                                                version >= Constants.TCP_PROTOCOL_VERSION_19 ? timeZoneOffset
                                                                                                             : timeZoneOffset
                                                                                                             * 60);
        }
        case NUMERIC:
            return ValueNumeric.get(new BigDecimal(readString(in)));
        case DOUBLE:
            return ValueDouble.get(readDouble(in));
        case REAL:
            return ValueReal.get(readFloat(in));
        case ENUM: {
            int ordinal = readInt(in);
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                return ((ExtTypeInfoEnum) columnType.getExtTypeInfo()).getValue(ordinal, session);
            }
            return ValueEnumBase.get(readString(in), ordinal);
        }
        case INTEGER:
            return ValueInteger.get(readInt(in));
        case BIGINT:
            return ValueBigint.get(readLong(in));
        case SMALLINT:
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                return ValueSmallint.get(readShort(in));
            } else {
                return ValueSmallint.get((short) readInt(in));
            }
        case VARCHAR:
            return ValueVarchar.get(readString(in));
        case VARCHAR_IGNORECASE:
            return ValueVarcharIgnoreCase.get(readString(in));
        case CHAR:
            return ValueChar.get(readString(in));
        case BLOB: {
            long length = readLong(in);
            if (length == -1) {
                // fetch-on-demand LOB
                int tableId = readInt(in);
                long id = readLong(in);
                byte[] hmac = readBytes(in);
                long precision = readLong(in);
                return new ValueBlob(new LobDataFetchOnDemand(session.getDataHandler(), tableId, id, hmac), precision);
            }
            Value v = session.getDataHandler().getLobStorage().createBlob(in, length);
            int magic = readInt(in);
            if (magic != LOB_MAGIC) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return v;
        }
        case CLOB: {
            long charLength = readLong(in);
            if (charLength == -1) {
                // fetch-on-demand LOB
                int tableId = readInt(in);
                long id = readLong(in);
                byte[] hmac = readBytes(in);
                long octetLength = version >= Constants.TCP_PROTOCOL_VERSION_20 ? readLong(in) : -1L;
                charLength = readLong(in);
                return new ValueClob(new LobDataFetchOnDemand(session.getDataHandler(), tableId, id, hmac), octetLength,
                                     charLength);
            }
            if (charLength < 0) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "length=" + charLength);
            }
            Value v = session.getDataHandler().getLobStorage().createClob(new DataReader(in), charLength);
            int magic = readInt(in);
            if (magic != LOB_MAGIC) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "magic=" + magic);
            }
            return v;
        }
        case ARRAY: {
            int len = readInt(in);
            if (len < 0) {
                // Unlikely, but possible with H2 1.4.200 and older versions
                len = ~len;
                readString(in);
            }
            if (columnType != null) {
                TypeInfo elementType = (TypeInfo) columnType.getExtTypeInfo();
                return ValueArray.get(elementType, readArrayElements(len, elementType, in), session);
            }
            return ValueArray.get(readArrayElements(len, null, in), session);
        }
        case ROW: {
            int len = readInt(in);
            Value[] list = new Value[len];
            if (columnType != null) {
                ExtTypeInfoRow extTypeInfoRow = (ExtTypeInfoRow) columnType.getExtTypeInfo();
                Iterator<Entry<String, TypeInfo>> fields = extTypeInfoRow.getFields().iterator();
                for (int i = 0; i < len; i++) {
                    list[i] = readValue(fields.next().getValue(), in);
                }
                return ValueRow.get(columnType, list);
            }
            for (int i = 0; i < len; i++) {
                list[i] = readValue(null, in);
            }
            return ValueRow.get(list);
        }
        case GEOMETRY:
            return ValueGeometry.get(readBytes(in));
        case INTERVAL: {
            int ordinal = readByte(in);
            boolean negative = ordinal < 0;
            if (negative) {
                ordinal = ~ordinal;
            }
            return ValueInterval.from(IntervalQualifier.valueOf(ordinal), negative, readLong(in),
                                      ordinal < 5 ? 0 : readLong(in));
        }
        case JSON:
            // Do not trust the value
            return ValueJson.fromJson(readBytes(in));
        case DECFLOAT: {
            String s = readString(in);
            switch (s) {
            case "-Infinity":
                return ValueDecfloat.NEGATIVE_INFINITY;
            case "Infinity":
                return ValueDecfloat.POSITIVE_INFINITY;
            case "NaN":
                return ValueDecfloat.NAN;
            default:
                return ValueDecfloat.get(new BigDecimal(s));
            }
        }
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    /**
     * Verify the HMAC.
     *
     * @param hmac  the message authentication code
     * @param lobId the lobId
     * @throws DbException if the HMAC does not match
     */
    public void verifyLobMac(byte[] hmac, long lobId) {
        byte[] result = calculateLobMac(lobId);
        if (!Utils.compareSecure(hmac, result)) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1,
                                  "Invalid lob hmac; possibly the connection was re-opened internally");
        }
    }

    public ByteString write(List<Value> values) {
        var baos = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(baos)) {
            int len = values == null ? 0 : values.size();
            writeInt(len, out);
            for (Value value : values) {
                writeValue(value, out);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Should nver have happened", e);
        }
        return ByteString.copyFrom(baos.toByteArray());
    }

    /**
     * Write a value.
     *
     * @param v the value
     * @throws IOException on failure
     */
    public void writeValue(Value v, DataOutputStream out) throws IOException {
        int type = v.getValueType();
        switch (type) {
        case Value.NULL:
            writeInt(NULL, out);
            break;
        case Value.BINARY:
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                writeInt(BINARY, out);
                writeBytes(v.getBytesNoCopy(), out);
                break;
            }
            //$FALL-THROUGH$
        case Value.VARBINARY:
            writeInt(VARBINARY, out);
            writeBytes(v.getBytesNoCopy(), out);
            break;
        case Value.JAVA_OBJECT:
            writeInt(JAVA_OBJECT, out);
            writeBytes(v.getBytesNoCopy(), out);
            break;
        case Value.UUID: {
            writeInt(UUID, out);
            ValueUuid uuid = (ValueUuid) v;
            writeLong(uuid.getHigh(), out);
            writeLong(uuid.getLow(), out);
            break;
        }
        case Value.BOOLEAN:
            writeInt(BOOLEAN, out);
            writeBoolean(v.getBoolean(), out);
            break;
        case Value.TINYINT:
            writeInt(TINYINT, out);
            writeByte(v.getByte(), out);
            break;
        case Value.TIME:
            writeInt(TIME, out);
            writeLong(((ValueTime) v).getNanos(), out);
            break;
        case Value.TIME_TZ: {
            ValueTimeTimeZone t = (ValueTimeTimeZone) v;
            if (version >= Constants.TCP_PROTOCOL_VERSION_19) {
                writeInt(TIME_TZ, out);
                writeLong(t.getNanos(), out);
                writeInt(t.getTimeZoneOffsetSeconds(), out);
            } else {
                writeInt(TIME, out);
                /*
                 * Don't call SessionRemote.currentTimestamp(), it may require own remote call
                 * and old server will not return custom time zone anyway.
                 */
                ValueTimestampTimeZone current = DateTimeUtils.currentTimestamp(DateTimeUtils.getTimeZone());
                writeLong(DateTimeUtils.normalizeNanosOfDay(t.getNanos()
                + (t.getTimeZoneOffsetSeconds() - current.getTimeZoneOffsetSeconds()) * DateTimeUtils.NANOS_PER_DAY),
                          out);
            }
            break;
        }
        case Value.DATE:
            writeInt(DATE, out);
            writeLong(((ValueDate) v).getDateValue(), out);
            break;
        case Value.TIMESTAMP: {
            writeInt(TIMESTAMP, out);
            ValueTimestamp ts = (ValueTimestamp) v;
            writeLong(ts.getDateValue(), out);
            writeLong(ts.getTimeNanos(), out);
            break;
        }
        case Value.TIMESTAMP_TZ: {
            writeInt(TIMESTAMP_TZ, out);
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v;
            writeLong(ts.getDateValue(), out);
            writeLong(ts.getTimeNanos(), out);
            int timeZoneOffset = ts.getTimeZoneOffsetSeconds();
            writeInt(version >= Constants.TCP_PROTOCOL_VERSION_19 //
                                                                 ? timeZoneOffset : timeZoneOffset / 60,
                     out);
            break;
        }
        case Value.DECFLOAT:
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                writeInt(DECFLOAT, out);
                writeString(v.getString(), out);
                break;
            }
            //$FALL-THROUGH$
        case Value.NUMERIC:
            writeInt(NUMERIC, out);
            writeString(v.getString(), out);
            break;
        case Value.DOUBLE:
            writeInt(DOUBLE, out);
            writeDouble(v.getDouble(), out);
            break;
        case Value.REAL:
            writeInt(REAL, out);
            writeFloat(v.getFloat(), out);
            break;
        case Value.INTEGER:
            writeInt(INTEGER, out);
            writeInt(v.getInt(), out);
            break;
        case Value.BIGINT:
            writeInt(BIGINT, out);
            writeLong(v.getLong(), out);
            break;
        case Value.SMALLINT:
            writeInt(SMALLINT, out);
            if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                writeShort(v.getShort(), out);
            } else {
                writeInt(v.getShort(), out);
            }
            break;
        case Value.VARCHAR:
            writeInt(VARCHAR, out);
            writeString(v.getString(), out);
            break;
        case Value.VARCHAR_IGNORECASE:
            writeInt(VARCHAR_IGNORECASE, out);
            writeString(v.getString(), out);
            break;
        case Value.CHAR:
            writeInt(CHAR, out);
            writeString(v.getString(), out);
            break;
        case Value.BLOB: {
            writeInt(BLOB, out);
            ValueBlob lob = (ValueBlob) v;
            LobData lobData = lob.getLobData();
            long length = lob.octetLength();
            if (lobData instanceof LobDataDatabase) {
                LobDataDatabase lobDataDatabase = (LobDataDatabase) lobData;
                writeLong(-1, out);
                writeInt(lobDataDatabase.getTableId(), out);
                writeLong(lobDataDatabase.getLobId(), out);
                writeBytes(calculateLobMac(lobDataDatabase.getLobId()), out);
                writeLong(length, out);
                break;
            }
            if (length < 0) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "length=" + length);
            }
            writeLong(length, out);
            long written = IOUtils.copyAndCloseInput(lob.getInputStream(), out);
            if (written != length) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "length:" + length + " written:" + written);
            }
            writeInt(LOB_MAGIC, out);
            break;
        }
        case Value.CLOB: {
            writeInt(CLOB, out);
            ValueClob lob = (ValueClob) v;
            LobData lobData = lob.getLobData();
            long charLength = lob.charLength();
            if (lobData instanceof LobDataDatabase) {
                LobDataDatabase lobDataDatabase = (LobDataDatabase) lobData;
                writeLong(-1, out);
                writeInt(lobDataDatabase.getTableId(), out);
                writeLong(lobDataDatabase.getLobId(), out);
                writeBytes(calculateLobMac(lobDataDatabase.getLobId()), out);
                if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
                    writeLong(lob.octetLength(), out);
                }
                writeLong(charLength, out);
                break;
            }
            if (charLength < 0) {
                throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "length=" + charLength);
            }
            writeLong(charLength, out);
            Reader reader = lob.getReader();
            Data.copyString(reader, out);
            writeInt(LOB_MAGIC, out);
            break;
        }
        case Value.ARRAY: {
            writeInt(ARRAY, out);
            ValueArray va = (ValueArray) v;
            Value[] list = va.getList();
            int len = list.length;
            writeInt(len, out);
            for (Value value : list) {
                writeValue(value, out);
            }
            break;
        }
        case Value.ROW: {
            writeInt(version >= Constants.TCP_PROTOCOL_VERSION_18 ? ROW : ARRAY, out);
            ValueRow va = (ValueRow) v;
            Value[] list = va.getList();
            int len = list.length;
            writeInt(len, out);
            for (Value value : list) {
                writeValue(value, out);
            }
            break;
        }
        case Value.ENUM: {
            writeInt(ENUM, out);
            writeInt(v.getInt(), out);
            if (version < Constants.TCP_PROTOCOL_VERSION_20) {
                writeString(v.getString(), out);
            }
            break;
        }
        case Value.GEOMETRY:
            writeInt(GEOMETRY, out);
            writeBytes(v.getBytesNoCopy(), out);
            break;
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
            if (version >= Constants.TCP_PROTOCOL_VERSION_18) {
                ValueInterval interval = (ValueInterval) v;
                int ordinal = type - Value.INTERVAL_YEAR;
                if (interval.isNegative()) {
                    ordinal = ~ordinal;
                }
                writeInt(INTERVAL, out);
                writeByte((byte) ordinal, out);
                writeLong(interval.getLeading(), out);
            } else {
                writeInt(VARCHAR, out);
                writeString(v.getString(), out);
            }
            break;
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            if (version >= Constants.TCP_PROTOCOL_VERSION_18) {
                ValueInterval interval = (ValueInterval) v;
                int ordinal = type - Value.INTERVAL_YEAR;
                if (interval.isNegative()) {
                    ordinal = ~ordinal;
                }
                writeInt(INTERVAL, out);
                writeByte((byte) ordinal, out);
                writeLong(interval.getLeading(), out);
                writeLong(interval.getRemaining(), out);
            } else {
                writeInt(VARCHAR, out);
                writeString(v.getString(), out);
            }
            break;
        case Value.JSON: {
            writeInt(JSON, out);
            writeBytes(v.getBytesNoCopy(), out);
            break;
        }
        default:
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "type=" + type);
        }
    }

    private byte[] calculateLobMac(long lobId) {
        byte[] data = new byte[8];
        Bits.writeLong(data, 0, lobId);
        return SHA256.getHashWithSalt(data, lobMacSalt);
    }

    private Value[] readArrayElements(int len, TypeInfo elementType, DataInputStream in) throws IOException {
        Value[] list = new Value[len];
        for (int i = 0; i < len; i++) {
            list[i] = readValue(elementType, in);
        }
        return list;
    }

    /**
     * Read a boolean.
     *
     * @return the value
     * @throws IOException on failure
     */
    private boolean readBoolean(DataInputStream in) throws IOException {
        return in.readByte() != 0;
    }

    /**
     * Read a byte.
     *
     * @return the value
     * @throws IOException on failure
     */
    private byte readByte(DataInputStream in) throws IOException {
        return in.readByte();
    }

    /**
     * Read a byte array.
     *
     * @return the value
     * @throws IOException on failure
     */
    private byte[] readBytes(DataInputStream in) throws IOException {
        int len = readInt(in);
        if (len == -1) {
            return null;
        }
        byte[] b = Utils.newBytes(len);
        in.readFully(b);
        return b;
    }

    /**
     * Read a double.
     *
     * @return the value
     * @throws IOException on failure
     */
    private double readDouble(DataInputStream in) throws IOException {
        return in.readDouble();
    }

    /**
     * Read a float.
     *
     * @return the value
     * @throws IOException on failure
     */
    private float readFloat(DataInputStream in) throws IOException {
        return in.readFloat();
    }

    /**
     * Read an int.
     *
     * @return the value
     * @throws IOException on failure
     */
    private int readInt(DataInputStream in) throws IOException {
        return in.readInt();
    }

    /**
     * Read a long.
     *
     * @return the value
     * @throws IOException on failure
     */
    private long readLong(DataInputStream in) throws IOException {
        return in.readLong();
    }

    /**
     * Read a short.
     *
     * @return the value
     * @throws IOException on failure
     */
    private short readShort(DataInputStream in) throws IOException {
        return in.readShort();
    }

    /**
     * Read a string.
     *
     * @return the value
     * @throws IOException on failure
     */
    private String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        }
        StringBuilder buff = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            buff.append(in.readChar());
        }
        String s = buff.toString();
        s = StringUtils.cache(s);
        return s;
    }

    /**
     * Read a type information.
     *
     * @return the type information
     * @throws IOException on failure
     */
    private TypeInfo readTypeInfo(DataInputStream in) throws IOException {
        if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
            return readTypeInfo20(in);
        } else {
            return readTypeInfo19(in);
        }
    }

    private TypeInfo readTypeInfo19(DataInputStream in) throws IOException {
        return TypeInfo.getTypeInfo(TI_TO_VALUE[readInt(in) + 1], readLong(in), readInt(in), null);
    }

    private TypeInfo readTypeInfo20(DataInputStream in) throws IOException {
        int valueType = TI_TO_VALUE[readInt(in) + 1];
        long precision = -1L;
        int scale = -1;
        ExtTypeInfo ext = null;
        switch (valueType) {
        case Value.UNKNOWN:
        case Value.NULL:
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
        case Value.DATE:
        case Value.UUID:
            break;
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.DECFLOAT:
        case Value.JAVA_OBJECT:
        case Value.JSON:
            precision = readInt(in);
            break;
        case Value.CLOB:
        case Value.BLOB:
            precision = readLong(in);
            break;
        case Value.NUMERIC:
            precision = readInt(in);
            scale = readInt(in);
            if (readBoolean(in)) {
                ext = ExtTypeInfoNumeric.DECIMAL;
            }
            break;
        case Value.REAL:
        case Value.DOUBLE:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
            precision = readByte(in);
            break;
        case Value.TIME:
        case Value.TIME_TZ:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            scale = readByte(in);
            break;
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            precision = readByte(in);
            scale = readByte(in);
            break;
        case Value.ENUM:
            ext = readTypeInfoEnum(in);
            break;
        case Value.GEOMETRY:
            ext = readTypeInfoGeometry(in);
            break;
        case Value.ARRAY:
            precision = readInt(in);
            ext = readTypeInfo(in);
            break;
        case Value.ROW:
            ext = readTypeInfoRow(in);
            break;
        default:
            throw DbException.getUnsupportedException("value type " + valueType);
        }
        return TypeInfo.getTypeInfo(valueType, precision, scale, ext);
    }

    private ExtTypeInfo readTypeInfoEnum(DataInputStream in) throws IOException {
        ExtTypeInfo ext;
        int c = readInt(in);
        if (c > 0) {
            String[] enumerators = new String[c];
            for (int i = 0; i < c; i++) {
                enumerators[i] = readString(in);
            }
            ext = new ExtTypeInfoEnum(enumerators);
        } else {
            ext = null;
        }
        return ext;
    }

    private ExtTypeInfo readTypeInfoGeometry(DataInputStream in) throws IOException {
        ExtTypeInfo ext;
        int e = readByte(in);
        switch (e) {
        case 0:
            ext = null;
            break;
        case 1:
            ext = new ExtTypeInfoGeometry(readShort(in), null);
            break;
        case 2:
            ext = new ExtTypeInfoGeometry(0, readInt(in));
            break;
        case 3:
            ext = new ExtTypeInfoGeometry(readShort(in), readInt(in));
            break;
        default:
            throw DbException.getUnsupportedException("GEOMETRY type encoding " + e);
        }
        return ext;
    }

    private ExtTypeInfo readTypeInfoRow(DataInputStream in) throws IOException {
        LinkedHashMap<String, TypeInfo> fields = new LinkedHashMap<>();
        for (int i = 0, l = readInt(in); i < l; i++) {
            String name = readString(in);
            if (fields.putIfAbsent(name, readTypeInfo(in)) != null) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, name);
            }
        }
        return new ExtTypeInfoRow(fields);
    }

    /**
     * Write a boolean.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeBoolean(boolean x, DataOutputStream out) throws IOException {
        out.writeByte((byte) (x ? 1 : 0));
        return this;
    }

    /**
     * Write a byte.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeByte(byte x, DataOutputStream out) throws IOException {
        out.writeByte(x);
        return this;
    }

    private void writeBytePrecisionWithDefault(long precision, DataOutputStream out) throws IOException {
        writeByte(precision >= 0 ? (byte) precision : -1, out);
    }

    /**
     * Write a byte array.
     *
     * @param data the value
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeBytes(byte[] data, DataOutputStream out) throws IOException {
        if (data == null) {
            writeInt(-1, out);
        } else {
            writeInt(data.length, out);
            out.write(data);
        }
        return this;
    }

    private void writeByteScaleWithDefault(int scale, DataOutputStream out) throws IOException {
        writeByte(scale >= 0 ? (byte) scale : -1, out);
    }

    /**
     * Write a double.
     *
     * @param i the value
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeDouble(double i, DataOutputStream out) throws IOException {
        out.writeDouble(i);
        return this;
    }

    /**
     * Write a float.
     *
     * @param i the value
     * @return itself
     */
    private StreamTransfer writeFloat(float i, DataOutputStream out) throws IOException {
        out.writeFloat(i);
        return this;
    }

    /**
     * Write an int.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeInt(int x, DataOutputStream out) throws IOException {
        out.writeInt(x);
        return this;
    }

    /**
     * Write a long.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeLong(long x, DataOutputStream out) throws IOException {
        out.writeLong(x);
        return this;
    }

    /**
     * Write a short.
     *
     * @param x the value
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeShort(short x, DataOutputStream out) throws IOException {
        out.writeShort(x);
        return this;
    }

    /**
     * Write a string. The maximum string length is Integer.MAX_VALUE.
     *
     * @param s the value
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeString(String s, DataOutputStream out) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(s.length());
            out.writeChars(s);
        }
        return this;
    }

    /**
     * Write value type, precision, and scale.
     *
     * @param type data type information
     * @return itself
     * @throws IOException on failure
     */
    private StreamTransfer writeTypeInfo(TypeInfo type, DataOutputStream out) throws IOException {
        if (version >= Constants.TCP_PROTOCOL_VERSION_20) {
            writeTypeInfo20(type, out);
        } else {
            writeTypeInfo19(type, out);
        }
        return this;
    }

    private void writeTypeInfo19(TypeInfo type, DataOutputStream out) throws IOException {
        int valueType = type.getValueType();
        switch (valueType) {
        case Value.BINARY:
            valueType = Value.VARBINARY;
            break;
        case Value.DECFLOAT:
            valueType = Value.NUMERIC;
            break;
        }
        writeInt(VALUE_TO_TI[valueType + 1], out).writeLong(type.getPrecision(), out).writeInt(type.getScale(), out);
    }

    private void writeTypeInfo20(TypeInfo type, DataOutputStream out) throws IOException {
        int valueType = type.getValueType();
        writeInt(VALUE_TO_TI[valueType + 1], out);
        switch (valueType) {
        case Value.UNKNOWN:
        case Value.NULL:
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
        case Value.DATE:
        case Value.UUID:
            break;
        case Value.CHAR:
        case Value.VARCHAR:
        case Value.VARCHAR_IGNORECASE:
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.DECFLOAT:
        case Value.JAVA_OBJECT:
        case Value.JSON:
            writeInt((int) type.getDeclaredPrecision(), out);
            break;
        case Value.CLOB:
        case Value.BLOB:
            writeLong(type.getDeclaredPrecision(), out);
            break;
        case Value.NUMERIC:
            writeInt((int) type.getDeclaredPrecision(), out);
            writeInt(type.getDeclaredScale(), out);
            writeBoolean(type.getExtTypeInfo() != null, out);
            break;
        case Value.REAL:
        case Value.DOUBLE:
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_MONTH:
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_YEAR_TO_MONTH:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_MINUTE:
            writeBytePrecisionWithDefault(type.getDeclaredPrecision(), out);
            break;
        case Value.TIME:
        case Value.TIME_TZ:
        case Value.TIMESTAMP:
        case Value.TIMESTAMP_TZ:
            writeByteScaleWithDefault(type.getDeclaredScale(), out);
            break;
        case Value.INTERVAL_SECOND:
        case Value.INTERVAL_DAY_TO_SECOND:
        case Value.INTERVAL_HOUR_TO_SECOND:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            writeBytePrecisionWithDefault(type.getDeclaredPrecision(), out);
            writeByteScaleWithDefault(type.getDeclaredScale(), out);
            break;
        case Value.ENUM:
            writeTypeInfoEnum(type, out);
            break;
        case Value.GEOMETRY:
            writeTypeInfoGeometry(type, out);
            break;
        case Value.ARRAY:
            writeInt((int) type.getDeclaredPrecision(), out);
            writeTypeInfo((TypeInfo) type.getExtTypeInfo(), out);
            break;
        case Value.ROW:
            writeTypeInfoRow(type, out);
            break;
        default:
            throw DbException.getUnsupportedException("value type " + valueType);
        }
    }

    private void writeTypeInfoEnum(TypeInfo type, DataOutputStream out) throws IOException {
        ExtTypeInfoEnum ext = (ExtTypeInfoEnum) type.getExtTypeInfo();
        if (ext != null) {
            int c = ext.getCount();
            writeInt(c, out);
            for (int i = 0; i < c; i++) {
                writeString(ext.getEnumerator(i), out);
            }
        } else {
            writeInt(0, out);
        }
    }

    private void writeTypeInfoGeometry(TypeInfo type, DataOutputStream out) throws IOException {
        ExtTypeInfoGeometry ext = (ExtTypeInfoGeometry) type.getExtTypeInfo();
        if (ext == null) {
            writeByte((byte) 0, out);
        } else {
            int t = ext.getType();
            Integer srid = ext.getSrid();
            if (t == 0) {
                if (srid == null) {
                    writeByte((byte) 0, out);
                } else {
                    writeByte((byte) 2, out);
                    writeInt(srid, out);
                }
            } else {
                if (srid == null) {
                    writeByte((byte) 1, out);
                    writeShort((short) t, out);
                } else {
                    writeByte((byte) 3, out);
                    writeShort((short) t, out);
                    writeInt(srid, out);
                }
            }
        }
    }

    private void writeTypeInfoRow(TypeInfo type, DataOutputStream out) throws IOException {
        Set<Map.Entry<String, TypeInfo>> fields = ((ExtTypeInfoRow) type.getExtTypeInfo()).getFields();
        writeInt(fields.size(), out);
        for (Map.Entry<String, TypeInfo> field : fields) {
            writeString(field.getKey(), out).writeTypeInfo(field.getValue(), out);
        }
    }

}
