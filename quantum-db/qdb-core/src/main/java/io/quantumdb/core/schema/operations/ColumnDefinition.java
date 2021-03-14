
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Arrays;
import com.google.common.base.Strings;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType;

public class ColumnDefinition {
    private final String        defaultValueExpression;
    private final Column.Hint[] hints;
    private final String        name;
    private final ColumnType    type;

    ColumnDefinition(String name, ColumnType type, Column.Hint... hints) {
        this(name, type, null, hints);
    }

    ColumnDefinition(String name, ColumnType type, String defaultValueExpression, Column.Hint... hints) {
        checkArgument(!Strings.isNullOrEmpty(name), "You must specify a \'name\'.");
        checkArgument(type != null, "You must specify a \'type\'.");
        checkArgument(hints != null, "You may not specify \'hints\' as NULL.");
        for (Column.Hint hint : hints) {
            checkArgument(hint != null, "You cannot add NULL as a hint.");
        }
        this.name = name;
        this.type = type;
        this.defaultValueExpression = defaultValueExpression;
        this.hints = hints;
    }

    @SuppressWarnings("unused")
    private ColumnDefinition() {
        this.name = null;
        this.type = null;
        this.defaultValueExpression = null;
        this.hints = new Column.Hint[0];
    }

    public Column createColumn() {
        return new Column(name, type, defaultValueExpression, hints);
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ColumnDefinition))
            return false;
        final ColumnDefinition other = (ColumnDefinition) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$name = this.getName();
        final java.lang.Object other$name = other.getName();
        if (this$name == null ? other$name != null : !this$name.equals(other$name))
            return false;
        final java.lang.Object this$type = this.getType();
        final java.lang.Object other$type = other.getType();
        if (this$type == null ? other$type != null : !this$type.equals(other$type))
            return false;
        final java.lang.Object this$defaultValueExpression = this.getDefaultValueExpression();
        final java.lang.Object other$defaultValueExpression = other.getDefaultValueExpression();
        if (this$defaultValueExpression == null ? other$defaultValueExpression != null
                : !this$defaultValueExpression.equals(other$defaultValueExpression))
            return false;
        if (!java.util.Arrays.deepEquals(this.getHints(), other.getHints()))
            return false;
        return true;
    }

    
    public String getDefaultValueExpression() {
        return this.defaultValueExpression;
    }

    
    public Column.Hint[] getHints() {
        return this.hints;
    }

    
    public String getName() {
        return this.name;
    }

    
    public ColumnType getType() {
        return this.type;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $name = this.getName();
        result = result * PRIME + ($name == null ? 43 : $name.hashCode());
        final java.lang.Object $type = this.getType();
        result = result * PRIME + ($type == null ? 43 : $type.hashCode());
        final java.lang.Object $defaultValueExpression = this.getDefaultValueExpression();
        result = result * PRIME + ($defaultValueExpression == null ? 43 : $defaultValueExpression.hashCode());
        result = result * PRIME + java.util.Arrays.deepHashCode(this.getHints());
        return result;
    }

    public boolean isAutoIncrement() {
        return containsHint(Column.Hint.AUTO_INCREMENT);
    }

    public boolean isIdentity() {
        return containsHint(Column.Hint.IDENTITY);
    }

    public boolean isNotNull() {
        return containsHint(Column.Hint.NOT_NULL);
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "ColumnDefinition(name=" + this.getName() + ", type=" + this.getType() + ", defaultValueExpression="
                + this.getDefaultValueExpression() + ", hints=" + java.util.Arrays.deepToString(this.getHints()) + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof ColumnDefinition;
    }

    private boolean containsHint(Column.Hint needle) {
        return Arrays.stream(hints).filter(hint -> hint == needle).findFirst().isPresent();
    }
}
