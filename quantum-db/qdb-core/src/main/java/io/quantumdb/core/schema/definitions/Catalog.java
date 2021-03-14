
package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class Catalog implements Copyable<Catalog> {
    private static final Logger log = LoggerFactory.getLogger(Catalog.class);
    
    private final String               name;
    private final Collection<Sequence> sequences;
    private final Collection<Table>    tables;
    private final Collection<View>     views;

    public Catalog(String name) {
        checkArgument(!Strings.isNullOrEmpty(name), "You must specify a \'name\'");
        this.name = name;
        this.tables = Sets.newTreeSet(Comparator.comparing(Table::getName));
        this.views = Sets.newTreeSet(Comparator.comparing(View::getName));
        this.sequences = Sets.newTreeSet(Comparator.comparing(Sequence::getName));
    }

    public Catalog addSequence(Sequence sequence) {
        checkArgument(sequence != null, "You must specify a \'sequence\'");
        sequences.add(sequence);
        sequence.setParent(this);
        return this;
    }

    public Catalog addTable(Table table) {
        checkArgument(table != null, "You must specify a \'table\'.");
        checkArgument(!containsTable(table.getName()),
                      "Catalog: \'" + name + "\' already contains a table: \'" + table.getName() + "\'.");
        checkArgument(!containsView(table.getName()),
                      "Catalog: \'" + name + "\' already contains a view: \'" + table.getName() + "\'.");
        checkArgument(!table.getColumns().isEmpty(),
                      "Table: \'" + table.getName() + "\' doesn\'t contain any columns.");
        checkArgument(!table.getIdentityColumns().isEmpty(),
                      "Table: \'" + table.getName() + "\' has no identity columns.");
        log.info("Adding table: {} to: {}", table.getName(), name);
        tables.add(table);
        table.setParent(this);
        return this;
    }

    public Catalog addView(View view) {
        checkArgument(view != null, "You must specify a \'view\'.");
        checkArgument(!containsTable(view.getName()),
                      "Catalog: \'" + name + "\' already contains a table: \'" + view.getName() + "\'.");
        checkArgument(!containsView(view.getName()),
                      "Catalog: \'" + name + "\' already contains a view: \'" + view.getName() + "\'.");
        view.setParent(this);
        views.add(view);
        return this;
    }

    public boolean containsTable(String tableName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'");
        return tables.stream().filter(t -> t.getName().equals(tableName)).findFirst().isPresent();
    }

    public boolean containsView(String viewName) {
        checkArgument(!Strings.isNullOrEmpty(viewName), "You must specify a \'viewName\'");
        return views.stream().filter(v -> v.getName().equals(viewName)).findFirst().isPresent();
    }

    @Override
    public Catalog copy() {
        Catalog schema = new Catalog(name);
        for (Table table : tables) {
            schema.addTable(table.copy());
        }
        for (View view : views) {
            schema.addView(view.copy());
        }
        for (ForeignKey foreignKey : getForeignKeys()) {
            Table source = schema.getTable(foreignKey.getReferencingTableName());
            Table target = schema.getTable(foreignKey.getReferredTableName());
            source.addForeignKey(foreignKey.getReferencingColumns())
                  .named(foreignKey.getForeignKeyName())
                  .onDelete(foreignKey.getOnDelete())
                  .onUpdate(foreignKey.getOnUpdate())
                  .referencing(target, foreignKey.getReferredColumns());
        }
        for (Sequence sequence : sequences) {
            schema.addSequence(sequence.copy());
        }
        return schema;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Catalog))
            return false;
        final Catalog other = (Catalog) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$name = this.getName();
        final java.lang.Object other$name = other.getName();
        if (this$name == null ? other$name != null : !this$name.equals(other$name))
            return false;
        final java.lang.Object this$tables = this.getTables();
        final java.lang.Object other$tables = other.getTables();
        if (this$tables == null ? other$tables != null : !this$tables.equals(other$tables))
            return false;
        final java.lang.Object this$views = this.getViews();
        final java.lang.Object other$views = other.getViews();
        if (this$views == null ? other$views != null : !this$views.equals(other$views))
            return false;
        final java.lang.Object this$sequences = this.getSequences();
        final java.lang.Object other$sequences = other.getSequences();
        if (this$sequences == null ? other$sequences != null : !this$sequences.equals(other$sequences))
            return false;
        return true;
    }

    public ImmutableSet<ForeignKey> getForeignKeys() {
        return ImmutableSet.copyOf(tables.stream()
                                         .flatMap(table -> table.getForeignKeys().stream())
                                         .collect(Collectors.toSet()));
    }

    public ImmutableSet<Index> getIndexes() {
        return ImmutableSet.copyOf(tables.stream()
                                         .flatMap(table -> table.getIndexes().stream())
                                         .collect(Collectors.toSet()));
    }

    
    public String getName() {
        return this.name;
    }

    
    public Collection<Sequence> getSequences() {
        return this.sequences;
    }

    public Table getTable(String tableName) { 
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'");
        return tables.stream()
                     .filter(t -> t.getName().equalsIgnoreCase(tableName))
                     .findFirst()
                     .orElseThrow(() -> new IllegalStateException(
                             "Catalog: " + name + " does not contain a table: " + tableName));
    }

    public ImmutableSet<Table> getTables() {
        return ImmutableSet.copyOf(tables);
    }

    public Set<String> getTablesReferencingTable(String tableName) {
        return tables.stream()
                     .filter(table -> table.referencesTable(tableName))
                     .map(Table::getName)
                     .collect(Collectors.toSet());
    }

    public View getView(String viewName) {
        checkArgument(!Strings.isNullOrEmpty(viewName), "You must specify a \'viewName\'");
        return views.stream()
                    .filter(v -> v.getName().equalsIgnoreCase(viewName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Catalog: " + name + " does not contain a view: " + viewName));
    }

    public ImmutableSet<View> getViews() {
        return ImmutableSet.copyOf(views);
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $name = this.getName();
        result = result * PRIME + ($name == null ? 43 : $name.hashCode());
        final java.lang.Object $tables = this.getTables();
        result = result * PRIME + ($tables == null ? 43 : $tables.hashCode());
        final java.lang.Object $views = this.getViews();
        result = result * PRIME + ($views == null ? 43 : $views.hashCode());
        final java.lang.Object $sequences = this.getSequences();
        result = result * PRIME + ($sequences == null ? 43 : $sequences.hashCode());
        return result;
    }

    public Sequence removeSequence(String sequenceName) {
        checkArgument(!Strings.isNullOrEmpty(sequenceName), "You must specify a \'sequenceName\'");
        Sequence sequence = getSequence(sequenceName);
        sequences.remove(sequence);
        sequence.setParent(null);
        tables.stream()
              .flatMap(table -> table.getColumns().stream())
              .filter(column -> sequence.equals(column.getSequence()))
              .forEach(Column::dropDefaultValue);
        return sequence;
    }

    public Table removeTable(String tableName) {
        Table table = getTable(tableName);
        table.canBeDropped();
        tables.remove(table);
        table.setParent(null);
        table.dropOutgoingForeignKeys();
        return table;
    }

    public View removeView(String viewName) {
        View view = getView(viewName);
        views.remove(view);
        view.setParent(null);
        return view;
    }

    @Override
    public String toString() {
        return PrettyPrinter.prettyPrint(this);
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof Catalog;
    }

    private Sequence getSequence(String sequenceName) {
        checkArgument(!Strings.isNullOrEmpty(sequenceName), "You must specify a \'sequenceName\'");
        return sequences.stream()
                        .filter(sequence -> sequence.getName().equals(sequenceName))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException(
                                "Catalog: " + name + " does not contain a sequence: " + sequenceName));
    }
}
