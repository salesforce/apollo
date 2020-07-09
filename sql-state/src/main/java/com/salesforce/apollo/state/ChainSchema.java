/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.util.Collection;
import java.util.Set;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

/**
 * A schema on a chain
 * 
 * @author hal.hildebrand
 *
 */
public class ChainSchema implements Schema {

    private byte[] hash;

    public byte[] getHash() {
        return hash;
    }

    public void setHash(byte[] hash) {
        this.hash = hash;
    }

    @Override
    public Table getTable(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RelProtoDataType getType(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> getTypeNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> getFunctionNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Schema getSubSchema(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isMutable() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        // TODO Auto-generated method stub
        return null;
    }

}
