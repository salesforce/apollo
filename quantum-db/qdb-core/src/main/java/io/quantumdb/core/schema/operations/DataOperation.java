
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

public class DataOperation implements Operation {
    private final String query;

    DataOperation(String query) {
        checkArgument(!Strings.isNullOrEmpty(query), "You must specify a \'query\'.");
        this.query = query;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DataOperation))
            return false;
        final DataOperation other = (DataOperation) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$query = this.getQuery();
        final java.lang.Object other$query = other.getQuery();
        if (this$query == null ? other$query != null : !this$query.equals(other$query))
            return false;
        return true;
    }

    
    public String getQuery() {
        return this.query;
    }

    @Override
    public Type getType() {
        return Type.DML;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $query = this.getQuery();
        result = result * PRIME + ($query == null ? 43 : $query.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "DataOperation(query=" + this.getQuery() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof DataOperation;
    }
}
