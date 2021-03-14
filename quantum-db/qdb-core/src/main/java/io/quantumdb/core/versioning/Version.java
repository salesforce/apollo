
package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import io.quantumdb.core.schema.operations.Operation;

/**
 * This class can be used to describe the evolution of the database at a given
 * point in time. It resembles much of the idea of a commit in the distributed
 * version control system Git, in such that it has a parent and that it holds
 * information on the difference between the parent Version and this Version.
 */
public class Version implements Comparable<Version> {
    private ChangeSet    changeSet;
    private Version      child;
    private final String id;
    private Operation    operation;
    private Version      parent;

    /**
     * Creates a new Version object based on the specified parameters.
     *
     * @param id     The unique identifier of this Version object.
     * @param parent The parent of this Version object (may be NULL in case of a
     *               root Version).
     */
    public Version(String id, Version parent) {
        this(id, parent, null, null);
    }

    /**
     * Creates a new Version object based on the specified parameters.
     *
     * @param id     The unique identifier of this Version object.
     * @param parent The parent of this Version object (may be NULL in case of a
     *               root Version).
     */
    public Version(String id, Version parent, ChangeSet changeSet, Operation operation) {
        checkArgument(!isNullOrEmpty(id), "You must specify a \'id\'.");
        this.id = id;
        this.operation = operation;
        this.changeSet = changeSet;
        if (parent != null) {
            this.parent = parent;
            this.parent.child = this;
        }
    }

    @Override
    public int compareTo(Version other) {
        if (this.equals(other)) {
            return 0;
        }
        Version pointer = this;
        while (pointer != null) {
            if (pointer.equals(other)) {
                return 1;
            }
            pointer = pointer.getParent();
        }
        pointer = this;
        while (pointer != null) {
            if (pointer.equals(other)) {
                return -1;
            }
            pointer = pointer.getChild();
        }
        return 0;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Version))
            return false;
        final Version other = (Version) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$id = this.getId();
        final java.lang.Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id))
            return false;
        return true;
    }

    
    public ChangeSet getChangeSet() {
        return this.changeSet;
    }

    
    public Version getChild() {
        return this.child;
    }

    
    public String getId() {
        return this.id;
    }

    
    public Operation getOperation() {
        return this.operation;
    }

    
    public Version getParent() {
        return this.parent;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        return result;
    }

    public boolean isRoot() {
        return parent == null;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "Version(id=" + this.getId() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof Version;
    }
}
