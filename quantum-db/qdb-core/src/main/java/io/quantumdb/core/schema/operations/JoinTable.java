
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.List;
import java.util.Map;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class JoinTable implements SchemaOperation {
    private final Map<String, String>       joinConditions;
    private final Map<String, List<String>> sourceColumns;
    private final Map<String, String>       sourceTables;
    private String                          targetTableName;

    JoinTable(String sourceTable, String alias, String... columns) {
        checkArgument(!Strings.isNullOrEmpty(sourceTable), "You must specify a \'sourceTable\'.");
        checkArgument(!Strings.isNullOrEmpty(alias), "You must specify a \'alias\'.");
        this.sourceTables = Maps.newLinkedHashMap();
        this.sourceColumns = Maps.newLinkedHashMap();
        this.joinConditions = Maps.newLinkedHashMap();
        sourceTables.put(alias, sourceTable);
        sourceColumns.put(alias, ImmutableList.copyOf(columns));
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof JoinTable))
            return false;
        final JoinTable other = (JoinTable) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$sourceTables = this.getSourceTables();
        final java.lang.Object other$sourceTables = other.getSourceTables();
        if (this$sourceTables == null ? other$sourceTables != null : !this$sourceTables.equals(other$sourceTables))
            return false;
        final java.lang.Object this$sourceColumns = this.getSourceColumns();
        final java.lang.Object other$sourceColumns = other.getSourceColumns();
        if (this$sourceColumns == null ? other$sourceColumns != null : !this$sourceColumns.equals(other$sourceColumns))
            return false;
        final java.lang.Object this$joinConditions = this.getJoinConditions();
        final java.lang.Object other$joinConditions = other.getJoinConditions();
        if (this$joinConditions == null ? other$joinConditions != null
                : !this$joinConditions.equals(other$joinConditions))
            return false;
        final java.lang.Object this$targetTableName = this.getTargetTableName();
        final java.lang.Object other$targetTableName = other.getTargetTableName();
        if (this$targetTableName == null ? other$targetTableName != null
                : !this$targetTableName.equals(other$targetTableName))
            return false;
        return true;
    }

    public ImmutableMap<String, String> getJoinConditions() {
        return ImmutableMap.copyOf(joinConditions);
    }

    public ImmutableMap<String, List<String>> getSourceColumns() {
        return ImmutableMap.copyOf(sourceColumns);
    }

    public ImmutableMap<String, String> getSourceTables() {
        return ImmutableMap.copyOf(sourceTables);
    }

    
    public String getTargetTableName() {
        return this.targetTableName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $sourceTables = this.getSourceTables();
        result = result * PRIME + ($sourceTables == null ? 43 : $sourceTables.hashCode());
        final java.lang.Object $sourceColumns = this.getSourceColumns();
        result = result * PRIME + ($sourceColumns == null ? 43 : $sourceColumns.hashCode());
        final java.lang.Object $joinConditions = this.getJoinConditions();
        result = result * PRIME + ($joinConditions == null ? 43 : $joinConditions.hashCode());
        final java.lang.Object $targetTableName = this.getTargetTableName();
        result = result * PRIME + ($targetTableName == null ? 43 : $targetTableName.hashCode());
        return result;
    }

    public JoinTable into(String tableName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        checkArgument(targetTableName == null, "You have already specified a target table.");
        checkArgument(!sourceColumns.isEmpty(), "You must specify which columns to join into the target table.");
        this.targetTableName = tableName;
        return this;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "JoinTable(sourceTables=" + this.getSourceTables() + ", sourceColumns=" + this.getSourceColumns()
                + ", joinConditions=" + this.getJoinConditions() + ", targetTableName=" + this.getTargetTableName()
                + ")";
    }

    public JoinTable with(String sourceTable, String alias, String joinCondition, String... columns) {
        checkArgument(!Strings.isNullOrEmpty(sourceTable), "You must specify a \'sourceTable\'.");
        checkArgument(!Strings.isNullOrEmpty(alias), "You must specify a \'alias\'.");
        checkArgument(!Strings.isNullOrEmpty(joinCondition), "You must specify a \'joinCondition\'.");
        checkArgument(!sourceTables.containsKey(alias), "You have ambiguously used the alias: \'" + alias + "\'.");
        this.sourceTables.put(alias, sourceTable);
        this.sourceColumns.put(alias, ImmutableList.copyOf(columns));
        this.joinConditions.put(alias, joinCondition);
        return this;
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof JoinTable;
    }
}
