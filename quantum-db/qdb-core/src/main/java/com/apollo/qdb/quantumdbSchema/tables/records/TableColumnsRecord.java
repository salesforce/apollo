/*
 * This file is generated by jOOQ.
 */
package com.apollo.qdb.quantumdbSchema.tables.records;


import com.apollo.qdb.quantumdbSchema.tables.TableColumns;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Row3;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class TableColumnsRecord extends UpdatableRecordImpl<TableColumnsRecord> implements Record3<Long, String, String> {

    private static final long serialVersionUID = -2033939323;

    /**
     * Setter for <code>QUANTUMDB.TABLE_COLUMNS.ID</code>.
     */
    public void setId(Long value) {
        set(0, value);
    }

    /**
     * Getter for <code>QUANTUMDB.TABLE_COLUMNS.ID</code>.
     */
    public Long getId() {
        return (Long) get(0);
    }

    /**
     * Setter for <code>QUANTUMDB.TABLE_COLUMNS.REF_ID</code>.
     */
    public void setRefId(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>QUANTUMDB.TABLE_COLUMNS.REF_ID</code>.
     */
    public String getRefId() {
        return (String) get(1);
    }

    /**
     * Setter for <code>QUANTUMDB.TABLE_COLUMNS.COLUMN_NAME</code>.
     */
    public void setColumnName(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>QUANTUMDB.TABLE_COLUMNS.COLUMN_NAME</code>.
     */
    public String getColumnName() {
        return (String) get(2);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<Long> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record3 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row3<Long, String, String> fieldsRow() {
        return (Row3) super.fieldsRow();
    }

    @Override
    public Row3<Long, String, String> valuesRow() {
        return (Row3) super.valuesRow();
    }

    @Override
    public Field<Long> field1() {
        return TableColumns.TABLE_COLUMNS.ID;
    }

    @Override
    public Field<String> field2() {
        return TableColumns.TABLE_COLUMNS.REF_ID;
    }

    @Override
    public Field<String> field3() {
        return TableColumns.TABLE_COLUMNS.COLUMN_NAME;
    }

    @Override
    public Long component1() {
        return getId();
    }

    @Override
    public String component2() {
        return getRefId();
    }

    @Override
    public String component3() {
        return getColumnName();
    }

    @Override
    public Long value1() {
        return getId();
    }

    @Override
    public String value2() {
        return getRefId();
    }

    @Override
    public String value3() {
        return getColumnName();
    }

    @Override
    public TableColumnsRecord value1(Long value) {
        setId(value);
        return this;
    }

    @Override
    public TableColumnsRecord value2(String value) {
        setRefId(value);
        return this;
    }

    @Override
    public TableColumnsRecord value3(String value) {
        setColumnName(value);
        return this;
    }

    @Override
    public TableColumnsRecord values(Long value1, String value2, String value3) {
        value1(value1);
        value2(value2);
        value3(value3);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached TableColumnsRecord
     */
    public TableColumnsRecord() {
        super(TableColumns.TABLE_COLUMNS);
    }

    /**
     * Create a detached, initialised TableColumnsRecord
     */
    public TableColumnsRecord(Long id, String refId, String columnName) {
        super(TableColumns.TABLE_COLUMNS);

        set(0, id);
        set(1, refId);
        set(2, columnName);
    }
}
