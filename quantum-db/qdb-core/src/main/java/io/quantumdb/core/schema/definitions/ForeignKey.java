
package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class ForeignKey {

    public static enum Action {
        CASCADE, NO_ACTION, RESTRICT, SET_DEFAULT, SET_NULL;
    }

    private final String                foreignKeyName;
    private Action                      onDelete;
    private Action                      onUpdate;
    private final ImmutableList<String> referencingColumns;
    private final Table                 referencingTable;
    private final ImmutableList<String> referredColumns;
    private final Table                 referredTable;

    ForeignKey(String foreignKeyName, Table referencingTable, List<String> referencingColumns, Table referredTable,
            List<String> referredColumns, Action onUpdate, Action onDelete) {
        checkArgument(!isNullOrEmpty(foreignKeyName), "You must specify a \'foreignKeyName\'.");
        checkArgument(onUpdate != null, "You must specify an \'onUpdate\' action.");
        checkArgument(onDelete != null, "You must specify an \'onDelete\' action.");
        checkArgument(referredColumns.size() == referencingColumns.size(),
                      "You must refer to as many columns as you are referring from.");
        for (String referencingColumn : referencingColumns) {
            checkArgument(referencingTable.containsColumn(referencingColumn), "The column: " + referencingColumn
                    + " is not present in table: " + referencingTable.getName());
        }
        for (String referredColumn : referredColumns) {
            checkArgument(referredTable.containsColumn(referredColumn),
                          "The column: " + referredColumn + " is not present in table: " + referredTable.getName());
        }
        this.foreignKeyName = foreignKeyName;
        this.referencingTable = referencingTable;
        this.referredTable = referredTable;
        this.referencingColumns = ImmutableList.copyOf(referencingColumns);
        this.referredColumns = ImmutableList.copyOf(referredColumns);
        this.onUpdate = onUpdate;
        this.onDelete = onDelete;
    }

    public boolean containsNonNullableColumns() {
        return referencingColumns.stream()
                                 .map(referencingTable::getColumn)
                                 .filter(Column::isNotNull)
                                 .findFirst()
                                 .map(column -> true)
                                 .orElse(false);
    }

    public void drop() {
        referencingColumns.stream().forEach(column -> referencingTable.getColumn(column).setOutgoingForeignKey(null));
        referredColumns.stream().forEach(column -> referredTable.getColumn(column).getIncomingForeignKeys().clear());
        referencingTable.dropForeignKey(this);
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ForeignKey))
            return false;
        final ForeignKey other = (ForeignKey) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$foreignKeyName = this.getForeignKeyName();
        final java.lang.Object other$foreignKeyName = other.getForeignKeyName();
        if (this$foreignKeyName == null ? other$foreignKeyName != null
                : !this$foreignKeyName.equals(other$foreignKeyName))
            return false;
        final java.lang.Object this$referencingTable = this.getReferencingTable();
        final java.lang.Object other$referencingTable = other.getReferencingTable();
        if (this$referencingTable == null ? other$referencingTable != null
                : !this$referencingTable.equals(other$referencingTable))
            return false;
        final java.lang.Object this$referredTable = this.getReferredTable();
        final java.lang.Object other$referredTable = other.getReferredTable();
        if (this$referredTable == null ? other$referredTable != null : !this$referredTable.equals(other$referredTable))
            return false;
        final java.lang.Object this$referencingColumns = this.getReferencingColumns();
        final java.lang.Object other$referencingColumns = other.getReferencingColumns();
        if (this$referencingColumns == null ? other$referencingColumns != null
                : !this$referencingColumns.equals(other$referencingColumns))
            return false;
        final java.lang.Object this$referredColumns = this.getReferredColumns();
        final java.lang.Object other$referredColumns = other.getReferredColumns();
        if (this$referredColumns == null ? other$referredColumns != null
                : !this$referredColumns.equals(other$referredColumns))
            return false;
        final java.lang.Object this$onUpdate = this.getOnUpdate();
        final java.lang.Object other$onUpdate = other.getOnUpdate();
        if (this$onUpdate == null ? other$onUpdate != null : !this$onUpdate.equals(other$onUpdate))
            return false;
        final java.lang.Object this$onDelete = this.getOnDelete();
        final java.lang.Object other$onDelete = other.getOnDelete();
        if (this$onDelete == null ? other$onDelete != null : !this$onDelete.equals(other$onDelete))
            return false;
        return true;
    }

    public LinkedHashMap<String, String> getColumnMapping() {
        LinkedHashMap<String, String> mapping = Maps.newLinkedHashMap();
        for (int i = 0; i < referencingColumns.size(); i++) {
            String referencingColumn = referencingColumns.get(i);
            String referredColumn = referredColumns.get(i);
            mapping.put(referencingColumn, referredColumn);
        }
        return mapping;
    }

    public Map<String, String> getColumns() {
        Map<String, String> columns = Maps.newLinkedHashMap();
        for (int i = 0; i < referredColumns.size(); i++) {
            String referencingColumnName = referencingColumns.get(i);
            String referredColumnName = referredColumns.get(i);
            columns.put(referencingColumnName, referredColumnName);
        }
        return columns;
    }

    
    public String getForeignKeyName() {
        return this.foreignKeyName;
    }

    
    public Action getOnDelete() {
        return this.onDelete;
    }

    
    public Action getOnUpdate() {
        return this.onUpdate;
    }

    public ImmutableList<String> getReferencingColumns() {
        return referencingColumns;
    }

    
    public Table getReferencingTable() {
        return this.referencingTable;
    }

    public String getReferencingTableName() {
        return referencingTable.getName();
    }

    public ImmutableList<String> getReferredColumns() {
        return referredColumns;
    }

    
    public Table getReferredTable() {
        return this.referredTable;
    }

    public String getReferredTableName() {
        return referredTable.getName();
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $foreignKeyName = this.getForeignKeyName();
        result = result * PRIME + ($foreignKeyName == null ? 43 : $foreignKeyName.hashCode());
        final java.lang.Object $referencingTable = this.getReferencingTable();
        result = result * PRIME + ($referencingTable == null ? 43 : $referencingTable.hashCode());
        final java.lang.Object $referredTable = this.getReferredTable();
        result = result * PRIME + ($referredTable == null ? 43 : $referredTable.hashCode());
        final java.lang.Object $referencingColumns = this.getReferencingColumns();
        result = result * PRIME + ($referencingColumns == null ? 43 : $referencingColumns.hashCode());
        final java.lang.Object $referredColumns = this.getReferredColumns();
        result = result * PRIME + ($referredColumns == null ? 43 : $referredColumns.hashCode());
        final java.lang.Object $onUpdate = this.getOnUpdate();
        result = result * PRIME + ($onUpdate == null ? 43 : $onUpdate.hashCode());
        final java.lang.Object $onDelete = this.getOnDelete();
        result = result * PRIME + ($onDelete == null ? 43 : $onDelete.hashCode());
        return result;
    }

    public boolean isInheritanceRelation() {
        Set<String> identityColumns = getReferencingTable().getIdentityColumns()
                                                           .stream()
                                                           .map(Column::getName)
                                                           .collect(Collectors.toSet());
        return referencingColumns.stream().anyMatch(identityColumns::contains);
    }

    public boolean isNotNullable() {
        return referencingColumns.stream().map(referencingTable::getColumn).anyMatch(Column::isNotNull);
    }

    public boolean isSelfReferencing() {
        return referencingTable.equals(referredTable);
    }

    
    public void setOnDelete(final Action onDelete) {
        this.onDelete = onDelete;
    }

    
    public void setOnUpdate(final Action onUpdate) {
        this.onUpdate = onUpdate;
    }

    @Override
    public String toString() {
        return PrettyPrinter.prettyPrint(this);
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof ForeignKey;
    }
}
