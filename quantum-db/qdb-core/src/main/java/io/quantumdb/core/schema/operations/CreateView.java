
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

/**
 * This SchemaOperation describes an operation which creates a new view.
 */
public class CreateView implements SchemaOperation {
    private String       query;
    private boolean      recursive = false;
    private boolean      temporary = false;
    private final String viewName;

    CreateView(String viewName) {
        checkArgument(!Strings.isNullOrEmpty(viewName), "You must specify a \'viewName\'.");
        this.viewName = viewName;
    }

    public CreateView as(String query) {
        this.query = query;
        return this;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof CreateView))
            return false;
        final CreateView other = (CreateView) o;
        if (!other.canEqual(this))
            return false;
        if (this.isTemporary() != other.isTemporary())
            return false;
        if (this.isRecursive() != other.isRecursive())
            return false;
        final java.lang.Object this$viewName = this.getViewName();
        final java.lang.Object other$viewName = other.getViewName();
        if (this$viewName == null ? other$viewName != null : !this$viewName.equals(other$viewName))
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

    
    public String getViewName() {
        return this.viewName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        result = result * PRIME + (this.isTemporary() ? 79 : 97);
        result = result * PRIME + (this.isRecursive() ? 79 : 97);
        final java.lang.Object $viewName = this.getViewName();
        result = result * PRIME + ($viewName == null ? 43 : $viewName.hashCode());
        final java.lang.Object $query = this.getQuery();
        result = result * PRIME + ($query == null ? 43 : $query.hashCode());
        return result;
    }

    
    public boolean isRecursive() {
        return this.recursive;
    }

    
    public boolean isTemporary() {
        return this.temporary;
    }

    public CreateView recursive() {
        this.recursive = true;
        return this;
    }

    public CreateView temporary() {
        this.temporary = true;
        return this;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "CreateView(viewName=" + this.getViewName() + ", temporary=" + this.isTemporary() + ", recursive="
                + this.isRecursive() + ", query=" + this.getQuery() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof CreateView;
    }
}
