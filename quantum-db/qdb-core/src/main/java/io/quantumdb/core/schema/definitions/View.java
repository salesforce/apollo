
package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.base.Strings;

public class View implements Copyable<View>, Comparable<View> {
    private String  name;
    private Catalog parent;
    private String  query;
    private boolean recursive;
    private boolean temporary;

    public View(String name, String query) {
        checkArgument(!Strings.isNullOrEmpty(name), "You must specify a \'name\'.");
        checkArgument(!Strings.isNullOrEmpty(query), "You must specify a \'query\'.");
        this.name = name;
        this.query = query;
    }

    @Override
    public int compareTo(View o) {
        return name.compareTo(o.name);
    }

    @Override
    public View copy() {
        View copy = new View(name, query);
        copy.recursive = recursive;
        copy.temporary = temporary;
        return copy;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof View))
            return false;
        final View other = (View) o;
        if (!other.canEqual(this))
            return false;
        if (this.isTemporary() != other.isTemporary())
            return false;
        if (this.isRecursive() != other.isRecursive())
            return false;
        final java.lang.Object this$name = this.getName();
        final java.lang.Object other$name = other.getName();
        if (this$name == null ? other$name != null : !this$name.equals(other$name))
            return false;
        final java.lang.Object this$query = this.getQuery();
        final java.lang.Object other$query = other.getQuery();
        if (this$query == null ? other$query != null : !this$query.equals(other$query))
            return false;
        return true;
    }

    
    public String getName() {
        return this.name;
    }

    
    public Catalog getParent() {
        return this.parent;
    }

    
    public String getQuery() {
        return this.query;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        result = result * PRIME + (this.isTemporary() ? 79 : 97);
        result = result * PRIME + (this.isRecursive() ? 79 : 97);
        final java.lang.Object $name = this.getName();
        result = result * PRIME + ($name == null ? 43 : $name.hashCode());
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

    public View rename(String newName) {
        checkArgument(!Strings.isNullOrEmpty(newName), "You must specify a \'name\'.");
        if (parent != null) {
            checkState(!parent.containsTable(newName),
                       "Catalog: " + parent.getName() + " already contains view with name: " + newName);
        }
        this.name = newName;
        return this;
    }

    @Override
    public String toString() {
        return PrettyPrinter.prettyPrint(this);
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof View;
    }

    void setParent(Catalog parent) {
        if (parent == null && this.parent != null) {
            checkState(!this.parent.containsView(name),
                       "The view: " + name + " is still present in the catalog: " + this.parent.getName() + ".");
        } else if (parent != null && this.parent == null) {
            checkState(!parent.containsTable(name), "The catalog: " + parent.getName()
                    + " already contains a different table with the name: " + name);
            checkState(!parent.containsView(name), "The catalog: " + parent.getName()
                    + " already contains a different view with the name: " + name);
        }
        this.parent = parent;
    }
}
