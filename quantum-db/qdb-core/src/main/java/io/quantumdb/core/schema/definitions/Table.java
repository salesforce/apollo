
package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.ForeignKey.Action;
import io.quantumdb.core.utils.RandomHasher;

public class Table implements Copyable<Table>, Comparable<Table> {

    public static class ForeignKeyBuilder {
        private String             name;
        private Action             onDelete;
        private Action             onUpdate;
        private final Table        parentTable;
        private final List<String> referringColumns;

        private ForeignKeyBuilder(Table parent, List<String> referringColumnNames) {
            this.name = "fk_" + RandomHasher.generateHash();
            this.parentTable = parent;
            this.referringColumns = referringColumnNames;
            this.onUpdate = Action.NO_ACTION;
            this.onDelete = Action.NO_ACTION;
        }

        public ForeignKeyBuilder named(String name) {
            this.name = name;
            return this;
        }

        public ForeignKeyBuilder onDelete(Action action) {
            this.onDelete = action;
            return this;
        }

        public ForeignKeyBuilder onUpdate(Action action) {
            this.onUpdate = action;
            return this;
        }

        public ForeignKey referencing(Table table, List<String> referredColumns) {
            ForeignKey constraint = new ForeignKey(name, parentTable, referringColumns, table, referredColumns,
                    onUpdate, onDelete);
            referringColumns.forEach(column -> parentTable.getColumn(column).setOutgoingForeignKey(constraint));
            referredColumns.forEach(column -> table.getColumn(column).getIncomingForeignKeys().add(constraint));
            parentTable.foreignKeys.add(constraint);
            return constraint;
        }

        public ForeignKey referencing(Table table, String... referredColumns) {
            return referencing(table, Lists.newArrayList(referredColumns));
        }
    }

    private final LinkedHashSet<Column> columns     = Sets.newLinkedHashSet();
    private final List<ForeignKey>      foreignKeys = Lists.newArrayList();
    private final List<Index>           indexes     = Lists.newArrayList();
    private String                      name;
    private Catalog                     parent;

    public Table(String name) {
        checkArgument(!Strings.isNullOrEmpty(name), "You must specify a \'name\'.");
        this.name = name;
    }

    public Table addColumn(Column column) {
        checkArgument(column != null, "You must specify a \'column\'.");
        checkState(!containsColumn(column.getName()), "Table already contains a column with name: " + column.getName());
        columns.add(column);
        column.setParent(this);
        return this;
    }

    public Table addColumns(Collection<Column> newColumns) {
        checkArgument(newColumns != null, "You must specify a \'newColumns\'.");
        newColumns.forEach(this::addColumn);
        return this;
    }

    public ForeignKeyBuilder addForeignKey(List<String> referringColumns) {
        return new ForeignKeyBuilder(this, referringColumns);
    }

    public ForeignKeyBuilder addForeignKey(String... referringColumns) {
        return addForeignKey(Lists.newArrayList(referringColumns));
    }

    public Table addIndex(Index index) {
        checkArgument(index != null, "You must specify an \'index\'.");
        checkState(!containsIndex(index.getIndexName()),
                   "Table already contains an index with name: " + index.getIndexName());
        indexes.add(index);
        index.setParent(this);
        return this;
    }

    @Override
    public int compareTo(Table o) {
        return name.compareTo(o.name);
    }

    public boolean containsColumn(String columnName) {
        checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a \'name\'.");
        return columns.stream().filter(c -> c.getName().equals(columnName)).findFirst().isPresent();
    }

    public boolean containsIndex(Collection<String> columns) {
        checkArgument(!columns.isEmpty(), "You must specify at least one entry in \'columns\'.");
        return indexes.stream().filter(c -> c.getColumns().equals(Lists.newArrayList(columns))).findFirst().isPresent();
    }

    public boolean containsIndex(String... columns) {
        return containsIndex(Sets.newHashSet(columns));
    }

    @Override
    public Table copy() {
        Table copy = new Table(name);
        columns.stream().forEachOrdered(column -> copy.addColumn(column.copy()));
        indexes.stream().forEachOrdered(index -> copy.addIndex(new Index(index.getColumns(), index.isUnique())));
        return copy;
    }

    public Set<String> enumerateReferencedByTables() {
        return parent.getTablesReferencingTable(getName());
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Table))
            return false;
        final Table other = (Table) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$name = this.getName();
        final java.lang.Object other$name = other.getName();
        if (this$name == null ? other$name != null : !this$name.equals(other$name))
            return false;
        final java.lang.Object this$columns = this.getColumns();
        final java.lang.Object other$columns = other.getColumns();
        if (this$columns == null ? other$columns != null : !this$columns.equals(other$columns))
            return false;
        return true;
    }

    public Column getColumn(String columnName) {
        checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a \'columnName\'.");
        return columns.stream()
                      .filter(c -> c.getName().equals(columnName))
                      .findFirst()
                      .orElseThrow(() -> new IllegalStateException(
                              "Table: " + name + " does not contain column: " + columnName));
    }

    public ImmutableList<Column> getColumns() {
        return ImmutableList.copyOf(columns);
    }

    
    public List<ForeignKey> getForeignKeys() {
        return this.foreignKeys;
    }

    public List<Column> getIdentityColumns() {
        return getColumns().stream().filter(Column::isIdentity).collect(Collectors.toList());
    }

    public Index getIndex(Collection<String> columns) {
        checkArgument(!columns.isEmpty(), "You must specify at least one \'columns\'.");
        return indexes.stream()
                      .filter(c -> c.getColumns().equals(Lists.newArrayList(columns)))
                      .findFirst()
                      .orElse(null);
    }

    public Index getIndex(String... columns) {
        return getIndex(Sets.newHashSet(columns));
    }

    public ImmutableList<Index> getIndexes() {
        return ImmutableList.copyOf(indexes);
    }

    
    public String getName() {
        return this.name;
    }

    
    public Catalog getParent() {
        return this.parent;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $name = this.getName();
        result = result * PRIME + ($name == null ? 43 : $name.hashCode());
        final java.lang.Object $columns = this.getColumns();
        result = result * PRIME + ($columns == null ? 43 : $columns.hashCode());
        return result;
    }

    public boolean referencesTable(String tableName) {
        return foreignKeys.stream()
                          .filter(foreignKey -> foreignKey.getReferredTableName().equals(tableName))
                          .findAny()
                          .isPresent();
    }

    public Column removeColumn(String columnName) {
        checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a \'name\'.");
        checkState(containsColumn(columnName), "You cannot remove a column which does not exist: " + columnName);
        Column column = getColumn(columnName);
        checkState(column.getIncomingForeignKeys().isEmpty(),
                   "You cannot remove a column that is still referenced by foreign keys.");
        List<Column> identityColumns = getIdentityColumns();
        identityColumns.remove(column);
        checkState(!identityColumns.isEmpty(), "You drop the last remaining identity column of a table.");
        if (column.getOutgoingForeignKey() != null) {
            column.getOutgoingForeignKey().drop();
        }
        column.setParent(null);
        columns.remove(column);
        return column;
    }

    public Index removeIndex(Collection<String> columns) {
        checkState(containsIndex(columns), "You cannot remove an index which does not exist: " + columns);
        Index index = getIndex(columns);
        index.setParent(null);
        indexes.remove(index);
        return index;
    }

    public Index removeIndex(String... columns) {
        return removeIndex(Sets.newHashSet(columns));
    }

    public Table rename(String newName) {
        checkArgument(!Strings.isNullOrEmpty(newName), "You must specify a \'name\'.");
        if (parent != null) {
            checkState(!parent.containsTable(newName),
                       "Catalog: " + parent.getName() + " already contains table with name: " + newName);
        }
        this.name = newName;
        return this;
    }

    @Override
    public String toString() {
        return PrettyPrinter.prettyPrint(this);
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof Table;
    }

    void canBeDropped() {
        for (Column column : columns) {
            checkState(column.getIncomingForeignKeys().isEmpty(),
                       "The column: " + column.getName() + " in table: " + name + " is still being referenced by "
                               + column.getIncomingForeignKeys().size() + " foreign key constraints.");
        }
    }

    void dropForeignKey(ForeignKey constraint) {
        foreignKeys.remove(constraint);
    }

    void dropOutgoingForeignKeys() {
        columns.stream()
               .filter(column -> column.getOutgoingForeignKey() != null)
               .map(column -> column.getOutgoingForeignKey())
               .distinct()
               .forEach(ForeignKey::drop);
    }

    void setParent(Catalog parent) {
        if (parent == null && this.parent != null) {
            checkState(!this.parent.containsTable(name),
                       "The table: " + name + " is still present in the catalog: " + this.parent.getName() + ".");
        } else if (parent != null && this.parent == null) {
            checkState(parent.containsTable(name) && this.equals(parent.getTable(name)), "The catalog: "
                    + parent.getName() + " already contains a different table with the name: " + name);
        }
        this.parent = parent;
    }
}
