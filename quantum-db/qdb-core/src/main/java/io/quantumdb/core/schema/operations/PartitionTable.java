
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Map;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class PartitionTable implements SchemaOperation {
    private final Map<String, String> partitions;
    private final String              tableName;

    PartitionTable(String tableName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        this.tableName = tableName;
        this.partitions = Maps.newLinkedHashMap();
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof PartitionTable))
            return false;
        final PartitionTable other = (PartitionTable) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        final java.lang.Object this$partitions = this.getPartitions();
        final java.lang.Object other$partitions = other.getPartitions();
        if (this$partitions == null ? other$partitions != null : !this$partitions.equals(other$partitions))
            return false;
        return true;
    }

    public ImmutableMap<String, String> getPartitions() {
        return ImmutableMap.copyOf(partitions);
    }

    
    public String getTableName() {
        return this.tableName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $tableName = this.getTableName();
        result = result * PRIME + ($tableName == null ? 43 : $tableName.hashCode());
        final java.lang.Object $partitions = this.getPartitions();
        result = result * PRIME + ($partitions == null ? 43 : $partitions.hashCode());
        return result;
    }

    public PartitionTable into(String tableName, String expression) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        checkArgument(!Strings.isNullOrEmpty(expression), "You must specify a \'expression\'.");
        checkArgument(!partitions.containsKey(tableName), "You cannot decompose into the same table multiple times.");
        partitions.put(tableName, expression);
        return this;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "PartitionTable(tableName=" + this.getTableName() + ", partitions=" + this.getPartitions() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof PartitionTable;
    }
}
