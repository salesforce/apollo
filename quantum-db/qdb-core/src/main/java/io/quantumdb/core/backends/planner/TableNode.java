
package io.quantumdb.core.backends.planner;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.base.Joiner;
import io.quantumdb.core.schema.definitions.ForeignKey;

public class TableNode {
    private final List<ForeignKey> foreignKeys;
    private final String           tableName;

    public TableNode(final String tableName, final List<ForeignKey> foreignKeys) {
        this.tableName = tableName;
        this.foreignKeys = foreignKeys;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof TableNode))
            return false;
        final TableNode other = (TableNode) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        return true;
    }

    public List<ForeignKey> getForeignKeys() {
        return this.foreignKeys;
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
        return result;
    }

    @Override
    public String toString() {
        if (foreignKeys.isEmpty()) {
            return tableName;
        }
        Set<String> referencedTables = foreignKeys.stream()
                                                  .map(ForeignKey::getReferredTableName)
                                                  .collect(Collectors.toSet());
        return tableName + ": [ " + Joiner.on(", ").join(referencedTables) + " ]";
    }

    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof TableNode;
    }
}
