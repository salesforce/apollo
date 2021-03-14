/*
 * This file is generated by jOOQ.
 */
package com.apollo.qdb.quantumdbSchema.tables.records;


import com.apollo.qdb.quantumdbSchema.tables.Synchronizers;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record5;
import org.jooq.Row5;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class SynchronizersRecord extends UpdatableRecordImpl<SynchronizersRecord> implements Record5<Long, String, String, String, String> {

    private static final long serialVersionUID = 1767258590;

    /**
     * Setter for <code>QUANTUMDB.SYNCHRONIZERS.ID</code>.
     */
    public void setId(Long value) {
        set(0, value);
    }

    /**
     * Getter for <code>QUANTUMDB.SYNCHRONIZERS.ID</code>.
     */
    public Long getId() {
        return (Long) get(0);
    }

    /**
     * Setter for <code>QUANTUMDB.SYNCHRONIZERS.SOURCE_REF_ID</code>.
     */
    public void setSourceRefId(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>QUANTUMDB.SYNCHRONIZERS.SOURCE_REF_ID</code>.
     */
    public String getSourceRefId() {
        return (String) get(1);
    }

    /**
     * Setter for <code>QUANTUMDB.SYNCHRONIZERS.TARGET_REF_ID</code>.
     */
    public void setTargetRefId(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>QUANTUMDB.SYNCHRONIZERS.TARGET_REF_ID</code>.
     */
    public String getTargetRefId() {
        return (String) get(2);
    }

    /**
     * Setter for <code>QUANTUMDB.SYNCHRONIZERS.FUNCTION_NAME</code>.
     */
    public void setFunctionName(String value) {
        set(3, value);
    }

    /**
     * Getter for <code>QUANTUMDB.SYNCHRONIZERS.FUNCTION_NAME</code>.
     */
    public String getFunctionName() {
        return (String) get(3);
    }

    /**
     * Setter for <code>QUANTUMDB.SYNCHRONIZERS.TRIGGER_NAME</code>.
     */
    public void setTriggerName(String value) {
        set(4, value);
    }

    /**
     * Getter for <code>QUANTUMDB.SYNCHRONIZERS.TRIGGER_NAME</code>.
     */
    public String getTriggerName() {
        return (String) get(4);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<Long> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record5 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row5<Long, String, String, String, String> fieldsRow() {
        return (Row5) super.fieldsRow();
    }

    @Override
    public Row5<Long, String, String, String, String> valuesRow() {
        return (Row5) super.valuesRow();
    }

    @Override
    public Field<Long> field1() {
        return Synchronizers.SYNCHRONIZERS.ID;
    }

    @Override
    public Field<String> field2() {
        return Synchronizers.SYNCHRONIZERS.SOURCE_REF_ID;
    }

    @Override
    public Field<String> field3() {
        return Synchronizers.SYNCHRONIZERS.TARGET_REF_ID;
    }

    @Override
    public Field<String> field4() {
        return Synchronizers.SYNCHRONIZERS.FUNCTION_NAME;
    }

    @Override
    public Field<String> field5() {
        return Synchronizers.SYNCHRONIZERS.TRIGGER_NAME;
    }

    @Override
    public Long component1() {
        return getId();
    }

    @Override
    public String component2() {
        return getSourceRefId();
    }

    @Override
    public String component3() {
        return getTargetRefId();
    }

    @Override
    public String component4() {
        return getFunctionName();
    }

    @Override
    public String component5() {
        return getTriggerName();
    }

    @Override
    public Long value1() {
        return getId();
    }

    @Override
    public String value2() {
        return getSourceRefId();
    }

    @Override
    public String value3() {
        return getTargetRefId();
    }

    @Override
    public String value4() {
        return getFunctionName();
    }

    @Override
    public String value5() {
        return getTriggerName();
    }

    @Override
    public SynchronizersRecord value1(Long value) {
        setId(value);
        return this;
    }

    @Override
    public SynchronizersRecord value2(String value) {
        setSourceRefId(value);
        return this;
    }

    @Override
    public SynchronizersRecord value3(String value) {
        setTargetRefId(value);
        return this;
    }

    @Override
    public SynchronizersRecord value4(String value) {
        setFunctionName(value);
        return this;
    }

    @Override
    public SynchronizersRecord value5(String value) {
        setTriggerName(value);
        return this;
    }

    @Override
    public SynchronizersRecord values(Long value1, String value2, String value3, String value4, String value5) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached SynchronizersRecord
     */
    public SynchronizersRecord() {
        super(Synchronizers.SYNCHRONIZERS);
    }

    /**
     * Create a detached, initialised SynchronizersRecord
     */
    public SynchronizersRecord(Long id, String sourceRefId, String targetRefId, String functionName, String triggerName) {
        super(Synchronizers.SYNCHRONIZERS);

        set(0, id);
        set(1, sourceRefId);
        set(2, targetRefId);
        set(3, functionName);
        set(4, triggerName);
    }
}
