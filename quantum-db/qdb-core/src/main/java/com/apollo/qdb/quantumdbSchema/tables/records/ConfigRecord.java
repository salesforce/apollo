/*
 * This file is generated by jOOQ.
 */
package com.apollo.qdb.quantumdbSchema.tables.records;


import com.apollo.qdb.quantumdbSchema.tables.Config;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Row2;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class ConfigRecord extends UpdatableRecordImpl<ConfigRecord> implements Record2<String, String> {

    private static final long serialVersionUID = -1451016489;

    /**
     * Setter for <code>QUANTUMDB.CONFIG.NAME</code>.
     */
    public void setName(String value) {
        set(0, value);
    }

    /**
     * Getter for <code>QUANTUMDB.CONFIG.NAME</code>.
     */
    public String getName() {
        return (String) get(0);
    }

    /**
     * Setter for <code>QUANTUMDB.CONFIG.VALUE</code>.
     */
    public void setValue(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>QUANTUMDB.CONFIG.VALUE</code>.
     */
    public String getValue() {
        return (String) get(1);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<String> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record2 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row2<String, String> fieldsRow() {
        return (Row2) super.fieldsRow();
    }

    @Override
    public Row2<String, String> valuesRow() {
        return (Row2) super.valuesRow();
    }

    @Override
    public Field<String> field1() {
        return Config.CONFIG.NAME;
    }

    @Override
    public Field<String> field2() {
        return Config.CONFIG.VALUE;
    }

    @Override
    public String component1() {
        return getName();
    }

    @Override
    public String component2() {
        return getValue();
    }

    @Override
    public String value1() {
        return getName();
    }

    @Override
    public String value2() {
        return getValue();
    }

    @Override
    public ConfigRecord value1(String value) {
        setName(value);
        return this;
    }

    @Override
    public ConfigRecord value2(String value) {
        setValue(value);
        return this;
    }

    @Override
    public ConfigRecord values(String value1, String value2) {
        value1(value1);
        value2(value2);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached ConfigRecord
     */
    public ConfigRecord() {
        super(Config.CONFIG);
    }

    /**
     * Create a detached, initialised ConfigRecord
     */
    public ConfigRecord(String name, String value) {
        super(Config.CONFIG);

        set(0, name);
        set(1, value);
    }
}
