
package io.quantumdb.core.schema.definitions;

public class Sequence {
    private String  name;
    private Catalog parent;

    public Sequence(String name) {
        this.name = name;
    }

    public Sequence copy() {
        return new Sequence(name);
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Sequence))
            return false;
        final Sequence other = (Sequence) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$name = this.getName();
        final java.lang.Object other$name = other.getName();
        if (this$name == null ? other$name != null : !this$name.equals(other$name))
            return false;
        return true;
    }

    
    public String getName() {
        return this.name;
    }

    
    public Catalog getParent() {
        return this.parent;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $name = this.getName();
        result = result * PRIME + ($name == null ? 43 : $name.hashCode());
        return result;
    }

    public Sequence rename(String newName) {
        this.name = newName;
        return this;
    }

    @Override
    public String toString() {
        return PrettyPrinter.prettyPrint(this);
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof Sequence;
    }

    void setParent(Catalog parent) {
        this.parent = parent;
    }
}
