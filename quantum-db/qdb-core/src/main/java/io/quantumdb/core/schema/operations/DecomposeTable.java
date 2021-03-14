
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Arrays;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

/**
 * This SchemaOperation describes an operation which creates one or more tables
 * consisting of specific columns from the source table.
 */
public class DecomposeTable implements SchemaOperation {
    private final Multimap<String, String> decompositions;
    private final String                   tableName;

    DecomposeTable(String tableName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        this.tableName = tableName;
        this.decompositions = LinkedHashMultimap.create();
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DecomposeTable))
            return false;
        final DecomposeTable other = (DecomposeTable) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        final java.lang.Object this$decompositions = this.getDecompositions();
        final java.lang.Object other$decompositions = other.getDecompositions();
        if (this$decompositions == null ? other$decompositions != null
                : !this$decompositions.equals(other$decompositions))
            return false;
        return true;
    }

    public ImmutableMultimap<String, String> getDecompositions() {
        return ImmutableMultimap.copyOf(decompositions);
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
        final java.lang.Object $decompositions = this.getDecompositions();
        result = result * PRIME + ($decompositions == null ? 43 : $decompositions.hashCode());
        return result;
    }

    public DecomposeTable into(String tableName, String... columns) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        checkArgument(columns.length != 0, "You must specify at least one column to decompose.");
        decompositions.putAll(tableName, Arrays.asList(columns));
        return this;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "DecomposeTable(tableName=" + this.getTableName() + ", decompositions=" + this.getDecompositions() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof DecomposeTable;
    }
}
