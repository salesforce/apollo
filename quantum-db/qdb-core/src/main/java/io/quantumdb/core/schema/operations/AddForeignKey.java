
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;
import io.quantumdb.core.schema.definitions.ForeignKey.Action;
import io.quantumdb.core.utils.RandomHasher;

public class AddForeignKey implements SchemaOperation {
    private String         name;
    private Action         onDelete;
    private Action         onUpdate;
    private String[]       referencedColumnNames;
    private String         referencedTableName;
    private final String[] referringColumnNames;
    private final String   referringTableName;

    AddForeignKey(String table, String... columns) {
        checkArgument(!Strings.isNullOrEmpty(table), "You must specify a \'table\'.");
        checkArgument(columns != null && columns.length > 0, "You must specify at least one \'column\'.");
        this.name = "fk_" + RandomHasher.generateHash();
        this.onDelete = Action.NO_ACTION;
        this.onUpdate = Action.NO_ACTION;
        this.referringTableName = table;
        this.referringColumnNames = columns;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof AddForeignKey))
            return false;
        final AddForeignKey other = (AddForeignKey) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$referringTableName = this.getReferringTableName();
        final java.lang.Object other$referringTableName = other.getReferringTableName();
        if (this$referringTableName == null ? other$referringTableName != null
                : !this$referringTableName.equals(other$referringTableName))
            return false;
        if (!java.util.Arrays.deepEquals(this.getReferringColumnNames(), other.getReferringColumnNames()))
            return false;
        final java.lang.Object this$name = this.getName();
        final java.lang.Object other$name = other.getName();
        if (this$name == null ? other$name != null : !this$name.equals(other$name))
            return false;
        final java.lang.Object this$onDelete = this.getOnDelete();
        final java.lang.Object other$onDelete = other.getOnDelete();
        if (this$onDelete == null ? other$onDelete != null : !this$onDelete.equals(other$onDelete))
            return false;
        final java.lang.Object this$onUpdate = this.getOnUpdate();
        final java.lang.Object other$onUpdate = other.getOnUpdate();
        if (this$onUpdate == null ? other$onUpdate != null : !this$onUpdate.equals(other$onUpdate))
            return false;
        final java.lang.Object this$referencedTableName = this.getReferencedTableName();
        final java.lang.Object other$referencedTableName = other.getReferencedTableName();
        if (this$referencedTableName == null ? other$referencedTableName != null
                : !this$referencedTableName.equals(other$referencedTableName))
            return false;
        if (!java.util.Arrays.deepEquals(this.getReferencedColumnNames(), other.getReferencedColumnNames()))
            return false;
        return true;
    }

    
    public String getName() {
        return this.name;
    }

    
    public Action getOnDelete() {
        return this.onDelete;
    }

    
    public Action getOnUpdate() {
        return this.onUpdate;
    }

    
    public String[] getReferencedColumnNames() {
        return this.referencedColumnNames;
    }

    
    public String getReferencedTableName() {
        return this.referencedTableName;
    }

    
    public String[] getReferringColumnNames() {
        return this.referringColumnNames;
    }

    
    public String getReferringTableName() {
        return this.referringTableName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $referringTableName = this.getReferringTableName();
        result = result * PRIME + ($referringTableName == null ? 43 : $referringTableName.hashCode());
        result = result * PRIME + java.util.Arrays.deepHashCode(this.getReferringColumnNames());
        final java.lang.Object $name = this.getName();
        result = result * PRIME + ($name == null ? 43 : $name.hashCode());
        final java.lang.Object $onDelete = this.getOnDelete();
        result = result * PRIME + ($onDelete == null ? 43 : $onDelete.hashCode());
        final java.lang.Object $onUpdate = this.getOnUpdate();
        result = result * PRIME + ($onUpdate == null ? 43 : $onUpdate.hashCode());
        final java.lang.Object $referencedTableName = this.getReferencedTableName();
        result = result * PRIME + ($referencedTableName == null ? 43 : $referencedTableName.hashCode());
        result = result * PRIME + java.util.Arrays.deepHashCode(this.getReferencedColumnNames());
        return result;
    }

    public AddForeignKey named(String name) {
        this.name = name;
        return this;
    }

    public AddForeignKey onDelete(Action action) {
        this.onDelete = action;
        return this;
    }

    public AddForeignKey onUpdate(Action action) {
        this.onUpdate = action;
        return this;
    }

    public AddForeignKey referencing(String table, String... columns) {
        checkArgument(!Strings.isNullOrEmpty(table), "You must specify a \'table\'.");
        checkArgument(columns != null && columns.length > 0, "You must specify at least one \'column\'.");
        checkArgument(columns.length == referringColumnNames.length, "The number of columns is mismatched.");
        this.referencedTableName = table;
        this.referencedColumnNames = columns;
        return this;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "AddForeignKey(referringTableName=" + this.getReferringTableName() + ", referringColumnNames="
                + java.util.Arrays.deepToString(this.getReferringColumnNames()) + ", name=" + this.getName()
                + ", onDelete=" + this.getOnDelete() + ", onUpdate=" + this.getOnUpdate() + ", referencedTableName="
                + this.getReferencedTableName() + ", referencedColumnNames="
                + java.util.Arrays.deepToString(this.getReferencedColumnNames()) + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof AddForeignKey;
    }
}
