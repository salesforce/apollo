
package io.quantumdb.core.backends.planner;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.quantumdb.core.schema.definitions.Table;

public class Operation {

    public enum Type {
        ADD_NULL, COPY, DROP_NULL;
    }

    private final LinkedHashSet<String> columns;
    private final Set<Table>            tables;
    private final Type                  type;

    public Operation(Set<Table> tables, Type type) {
        this(tables, Sets.newLinkedHashSet(), type);
    }

    public Operation(Table table, LinkedHashSet<String> columns, Type type) {
        this(Sets.newHashSet(table), columns, type);
    }

    private Operation(Set<Table> tables, LinkedHashSet<String> columns, Type type) {
        this.tables = tables;
        this.columns = columns;
        this.type = type;
    }

    public void addTable(Table table) {
        tables.add(table);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Operation) {
            Operation op = (Operation) other;
            return new EqualsBuilder().append(getTableNames(), op.getTableNames())
                                      .append(columns, op.columns)
                                      .append(type, op.type)
                                      .isEquals();
        }
        return false;
    }

    
    public LinkedHashSet<String> getColumns() {
        return this.columns;
    }

    public ImmutableSet<Table> getTables() {
        return ImmutableSet.copyOf(tables);
    }

    
    public Type getType() {
        return this.type;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getTableNames()).append(columns).append(type).toHashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(type.name());
        builder.append(" ");
        builder.append(getTableNames());
        if (columns != null && !columns.isEmpty()) {
            builder.append(" " + columns);
        }
        return builder.toString();
    }

    private Set<String> getTableNames() {
        return tables.stream().map(Table::getName).collect(Collectors.toSet());
    }
}
