/*
 * This file is generated by jOOQ.
 */
package com.apollo.qdb.quantumdbSchema.tables;


import com.apollo.qdb.quantumdbSchema.Keys;
import com.apollo.qdb.quantumdbSchema.Quantumdb;
import com.apollo.qdb.quantumdbSchema.tables.records.RefsRecord;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Row1;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Refs extends TableImpl<RefsRecord> {

    private static final long serialVersionUID = 56268197;

    /**
     * The reference instance of <code>QUANTUMDB.REFS</code>
     */
    public static final Refs REFS = new Refs();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<RefsRecord> getRecordType() {
        return RefsRecord.class;
    }

    /**
     * The column <code>QUANTUMDB.REFS.REF_ID</code>.
     */
    public final TableField<RefsRecord, String> REF_ID = createField(DSL.name("REF_ID"), org.jooq.impl.SQLDataType.VARCHAR(255).nullable(false), this, "");

    /**
     * Create a <code>QUANTUMDB.REFS</code> table reference
     */
    public Refs() {
        this(DSL.name("REFS"), null);
    }

    /**
     * Create an aliased <code>QUANTUMDB.REFS</code> table reference
     */
    public Refs(String alias) {
        this(DSL.name(alias), REFS);
    }

    /**
     * Create an aliased <code>QUANTUMDB.REFS</code> table reference
     */
    public Refs(Name alias) {
        this(alias, REFS);
    }

    private Refs(Name alias, Table<RefsRecord> aliased) {
        this(alias, aliased, null);
    }

    private Refs(Name alias, Table<RefsRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    public <O extends Record> Refs(Table<O> child, ForeignKey<O, RefsRecord> key) {
        super(child, key, REFS);
    }

    @Override
    public Schema getSchema() {
        return Quantumdb.QUANTUMDB;
    }

    @Override
    public UniqueKey<RefsRecord> getPrimaryKey() {
        return Keys.CONSTRAINT_2;
    }

    @Override
    public List<UniqueKey<RefsRecord>> getKeys() {
        return Arrays.<UniqueKey<RefsRecord>>asList(Keys.CONSTRAINT_2);
    }

    @Override
    public Refs as(String alias) {
        return new Refs(DSL.name(alias), this);
    }

    @Override
    public Refs as(Name alias) {
        return new Refs(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public Refs rename(String name) {
        return new Refs(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public Refs rename(Name name) {
        return new Refs(name, null);
    }

    // -------------------------------------------------------------------------
    // Row1 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row1<String> fieldsRow() {
        return (Row1) super.fieldsRow();
    }
}
