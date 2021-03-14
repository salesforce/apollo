
package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import java.util.List;
import com.google.common.collect.ImmutableList;
import io.quantumdb.core.utils.RandomHasher;

public class Index {
    private final ImmutableList<String> columns;
    private final String                indexName;
    private Table                       parent;
    private final boolean               unique;

    public Index(List<String> columns, boolean unique) {
        this("idx_" + RandomHasher.generateHash(), columns, unique);
    }

    public Index(String indexName, List<String> columns, boolean unique) {
        checkArgument(!isNullOrEmpty(indexName), "You must specify a \'foreignKeyName\'.");
        checkArgument(columns != null && !columns.isEmpty(), "You must specify at least one column.");
        this.indexName = indexName;
        this.columns = ImmutableList.copyOf(columns);
        this.unique = unique;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Index))
            return false;
        final Index other = (Index) o;
        if (!other.canEqual(this))
            return false;
        if (this.isUnique() != other.isUnique())
            return false;
        final java.lang.Object this$indexName = this.getIndexName();
        final java.lang.Object other$indexName = other.getIndexName();
        if (this$indexName == null ? other$indexName != null : !this$indexName.equals(other$indexName))
            return false;
        final java.lang.Object this$columns = this.getColumns();
        final java.lang.Object other$columns = other.getColumns();
        if (this$columns == null ? other$columns != null : !this$columns.equals(other$columns))
            return false;
        final java.lang.Object this$parent = this.getParent();
        final java.lang.Object other$parent = other.getParent();
        if (this$parent == null ? other$parent != null : !this$parent.equals(other$parent))
            return false;
        return true;
    }

    
    public ImmutableList<String> getColumns() {
        return this.columns;
    }

    
    public String getIndexName() {
        return this.indexName;
    }

    
    public Table getParent() {
        return this.parent;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        result = result * PRIME + (this.isUnique() ? 79 : 97);
        final java.lang.Object $indexName = this.getIndexName();
        result = result * PRIME + ($indexName == null ? 43 : $indexName.hashCode());
        final java.lang.Object $columns = this.getColumns();
        result = result * PRIME + ($columns == null ? 43 : $columns.hashCode());
        final java.lang.Object $parent = this.getParent();
        result = result * PRIME + ($parent == null ? 43 : $parent.hashCode());
        return result;
    }

    
    public boolean isUnique() {
        return this.unique;
    }

    
    public void setParent(final Table parent) {
        this.parent = parent;
    }

    @Override
    public String toString() {
        return PrettyPrinter.prettyPrint(this);
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof Index;
    }
}
