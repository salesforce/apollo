
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

/**
 * This SchemaOperation describes an operation which drops an existing view from
 * a catalog.
 */
public class DropView implements SchemaOperation {
    private final String viewName;

    DropView(String viewName) {
        checkArgument(!Strings.isNullOrEmpty(viewName), "You must specify a \'viewName\'.");
        this.viewName = viewName;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DropView))
            return false;
        final DropView other = (DropView) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$viewName = this.getViewName();
        final java.lang.Object other$viewName = other.getViewName();
        if (this$viewName == null ? other$viewName != null : !this$viewName.equals(other$viewName))
            return false;
        return true;
    }

    
    public String getViewName() {
        return this.viewName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $viewName = this.getViewName();
        result = result * PRIME + ($viewName == null ? 43 : $viewName.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "DropView(viewName=" + this.getViewName() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof DropView;
    }
}
