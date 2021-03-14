
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import java.util.Optional;
import java.util.Set;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;

/**
 * This SchemaOperation describes an operation which alters an already existing
 * column.
 */
public class AlterColumn implements SchemaOperation {
    private final String         columnName;
    private final Set<Hint>      hintsToAdd;
    private final Set<Hint>      hintsToDrop;
    private Optional<String>     newColumnName;
    private Optional<ColumnType> newColumnType;
    private Optional<String>     newDefaultValueExpression;
    private final String         tableName;

    AlterColumn(String tableName, String columnName) {
        checkArgument(!isNullOrEmpty(tableName), "You must specify a \'tableName\'");
        checkArgument(!isNullOrEmpty(columnName), "You must specify a \'columnName\'");
        this.tableName = tableName;
        this.columnName = columnName;
        this.hintsToDrop = Sets.newHashSet();
        this.hintsToAdd = Sets.newHashSet();
        this.newColumnName = Optional.empty();
        this.newColumnType = Optional.empty();
        this.newDefaultValueExpression = Optional.empty();
    }

    public AlterColumn addHint(Hint hint) {
        checkArgument(hint != null, "You must specify a \'hint\'.");
        this.hintsToAdd.add(hint);
        this.hintsToDrop.remove(hint);
        return this;
    }

    public AlterColumn dropDefaultExpression() {
        this.newDefaultValueExpression = Optional.ofNullable("");
        return this;
    }

    public AlterColumn dropHint(Hint hint) {
        checkArgument(hint != null, "You must specify a \'hint\'.");
        this.hintsToDrop.add(hint);
        this.hintsToAdd.remove(hint);
        return this;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof AlterColumn))
            return false;
        final AlterColumn other = (AlterColumn) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        final java.lang.Object this$columnName = this.getColumnName();
        final java.lang.Object other$columnName = other.getColumnName();
        if (this$columnName == null ? other$columnName != null : !this$columnName.equals(other$columnName))
            return false;
        final java.lang.Object this$hintsToDrop = this.getHintsToDrop();
        final java.lang.Object other$hintsToDrop = other.getHintsToDrop();
        if (this$hintsToDrop == null ? other$hintsToDrop != null : !this$hintsToDrop.equals(other$hintsToDrop))
            return false;
        final java.lang.Object this$hintsToAdd = this.getHintsToAdd();
        final java.lang.Object other$hintsToAdd = other.getHintsToAdd();
        if (this$hintsToAdd == null ? other$hintsToAdd != null : !this$hintsToAdd.equals(other$hintsToAdd))
            return false;
        final java.lang.Object this$newColumnName = this.getNewColumnName();
        final java.lang.Object other$newColumnName = other.getNewColumnName();
        if (this$newColumnName == null ? other$newColumnName != null : !this$newColumnName.equals(other$newColumnName))
            return false;
        final java.lang.Object this$newColumnType = this.getNewColumnType();
        final java.lang.Object other$newColumnType = other.getNewColumnType();
        if (this$newColumnType == null ? other$newColumnType != null : !this$newColumnType.equals(other$newColumnType))
            return false;
        final java.lang.Object this$newDefaultValueExpression = this.getNewDefaultValueExpression();
        final java.lang.Object other$newDefaultValueExpression = other.getNewDefaultValueExpression();
        if (this$newDefaultValueExpression == null ? other$newDefaultValueExpression != null
                : !this$newDefaultValueExpression.equals(other$newDefaultValueExpression))
            return false;
        return true;
    }

    
    public String getColumnName() {
        return this.columnName;
    }

    public ImmutableSet<Hint> getHintsToAdd() {
        return ImmutableSet.copyOf(hintsToAdd);
    }

    public ImmutableSet<Hint> getHintsToDrop() {
        return ImmutableSet.copyOf(hintsToDrop);
    }

    
    public Optional<String> getNewColumnName() {
        return this.newColumnName;
    }

    
    public Optional<ColumnType> getNewColumnType() {
        return this.newColumnType;
    }

    
    public Optional<String> getNewDefaultValueExpression() {
        return this.newDefaultValueExpression;
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
        final java.lang.Object $columnName = this.getColumnName();
        result = result * PRIME + ($columnName == null ? 43 : $columnName.hashCode());
        final java.lang.Object $hintsToDrop = this.getHintsToDrop();
        result = result * PRIME + ($hintsToDrop == null ? 43 : $hintsToDrop.hashCode());
        final java.lang.Object $hintsToAdd = this.getHintsToAdd();
        result = result * PRIME + ($hintsToAdd == null ? 43 : $hintsToAdd.hashCode());
        final java.lang.Object $newColumnName = this.getNewColumnName();
        result = result * PRIME + ($newColumnName == null ? 43 : $newColumnName.hashCode());
        final java.lang.Object $newColumnType = this.getNewColumnType();
        result = result * PRIME + ($newColumnType == null ? 43 : $newColumnType.hashCode());
        final java.lang.Object $newDefaultValueExpression = this.getNewDefaultValueExpression();
        result = result * PRIME + ($newDefaultValueExpression == null ? 43 : $newDefaultValueExpression.hashCode());
        return result;
    }

    public AlterColumn modifyDataType(ColumnType newColumnType) {
        checkArgument(newColumnType != null, "You must specify a \'newColumnType\'.");
        this.newColumnType = Optional.of(newColumnType);
        return this;
    }

    public AlterColumn modifyDefaultExpression(String newDefaultExpression) {
        checkArgument(!isNullOrEmpty(newDefaultExpression), "You must specify a \'newDefaultExpression\'.");
        this.newDefaultValueExpression = Optional.of(newDefaultExpression);
        return this;
    }

    public AlterColumn rename(String newColumnName) {
        checkArgument(!isNullOrEmpty(newColumnName), "You must specify a \'newColumnNameName\'.");
        this.newColumnName = Optional.of(newColumnName);
        return this;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "AlterColumn(tableName=" + this.getTableName() + ", columnName=" + this.getColumnName()
                + ", hintsToDrop=" + this.getHintsToDrop() + ", hintsToAdd=" + this.getHintsToAdd() + ", newColumnName="
                + this.getNewColumnName() + ", newColumnType=" + this.getNewColumnType()
                + ", newDefaultValueExpression=" + this.getNewDefaultValueExpression() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof AlterColumn;
    }
}
