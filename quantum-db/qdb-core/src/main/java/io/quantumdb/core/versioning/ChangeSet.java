
package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import java.util.Comparator;
import java.util.Date;

/**
 * This class describes a list of SchemaOperations which batched together form a
 * logical set of changes to the database schema. This batch can optionally be
 * given a description, much like a commit message in version control systems.
 */
public class ChangeSet implements Comparable<ChangeSet> {
    private final String author;
    private final Date   created;
    private final String description;
    private final String id;
    private Version      version;

    /**
     * Creates a new ChangeSet object based on the specified description and array
     * of SchemaOperations.
     *
     * @param author The author of the ChangeSet.
     */
    public ChangeSet(String id, String author) {
        this(id, author, null);
    }

    /**
     * Creates a new ChangeSet object based on the specified description and array
     * of SchemaOperations.
     *
     * @param author      The author of the ChangeSet.
     * @param description The description of the ChangeSet (may be NULL).
     */
    public ChangeSet(String id, String author, String description) {
        this(id, author, new Date(), description);
    }

    /**
     * Creates a new ChangeSet object based on the specified description and array
     * of SchemaOperations.
     *
     * @param author      The author of the ChangeSet.
     * @param created     The time of creation of this ChangeSet.
     * @param description The description of the ChangeSet (may be NULL).
     */
    ChangeSet(String id, String author, Date created, String description) {
        checkArgument(!isNullOrEmpty(author), "You must specify an \'author\'.");
        checkArgument(created != null, "You must specify a \'created\' Date.");
        this.id = id;
        this.author = author;
        this.created = created;
        this.description = emptyToNull(description);
    }

    @Override
    public int compareTo(ChangeSet o) {
        return Comparator.comparing(ChangeSet::getCreated).compare(this, o);
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ChangeSet))
            return false;
        final ChangeSet other = (ChangeSet) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$id = this.getId();
        final java.lang.Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id))
            return false;
        final java.lang.Object this$author = this.getAuthor();
        final java.lang.Object other$author = other.getAuthor();
        if (this$author == null ? other$author != null : !this$author.equals(other$author))
            return false;
        final java.lang.Object this$created = this.getCreated();
        final java.lang.Object other$created = other.getCreated();
        if (this$created == null ? other$created != null : !this$created.equals(other$created))
            return false;
        final java.lang.Object this$description = this.getDescription();
        final java.lang.Object other$description = other.getDescription();
        if (this$description == null ? other$description != null : !this$description.equals(other$description))
            return false;
        final java.lang.Object this$version = this.getVersion();
        final java.lang.Object other$version = other.getVersion();
        if (this$version == null ? other$version != null : !this$version.equals(other$version))
            return false;
        return true;
    }

    
    public String getAuthor() {
        return this.author;
    }

    
    public Date getCreated() {
        return this.created;
    }

    
    public String getDescription() {
        return this.description;
    }

    
    public String getId() {
        return this.id;
    }

    
    public Version getVersion() {
        return this.version;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final java.lang.Object $author = this.getAuthor();
        result = result * PRIME + ($author == null ? 43 : $author.hashCode());
        final java.lang.Object $created = this.getCreated();
        result = result * PRIME + ($created == null ? 43 : $created.hashCode());
        final java.lang.Object $description = this.getDescription();
        result = result * PRIME + ($description == null ? 43 : $description.hashCode());
        final java.lang.Object $version = this.getVersion();
        result = result * PRIME + ($version == null ? 43 : $version.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "ChangeSet(id=" + this.getId() + ", author=" + this.getAuthor() + ", created=" + this.getCreated()
                + ", description=" + this.getDescription() + ", version=" + this.getVersion() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof ChangeSet;
    }

    
    void setVersion(final Version version) {
        this.version = version;
    }
}
