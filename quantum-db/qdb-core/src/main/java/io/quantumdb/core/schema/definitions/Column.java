
package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.List;
import java.util.Set;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class Column implements Copyable<Column> {

    public static enum Hint {
        AUTO_INCREMENT, IDENTITY, NOT_NULL;
    }

    private String                 defaultValue;
    private final Set<Hint>        hints;
    private final List<ForeignKey> incomingForeignKeys;
    private String                 name;
    private ForeignKey             outgoingForeignKey;
    private Table                  parent;
    private Sequence               sequence;
    private ColumnType             type;

    public Column(String name, ColumnType type, Hint... hints) {
        this(name, type, null, null, hints);
    }

    public Column(String name, ColumnType type, Sequence sequence, Hint... hints) {
        this(name, type, sequence, null, hints);
    }

    public Column(String name, ColumnType type, String defaultValue, Hint... hints) {
        this(name, type, null, defaultValue, hints);
    }

    private Column(String name, ColumnType type, Sequence sequence, String defaultValueExpression, Hint... hints) {
        checkArgument(!Strings.isNullOrEmpty(name), "You must specify a \'name\'.");
        checkArgument(type != null, "You must specify a \'type\'.");
        checkArgument(hints != null, "You may not specify \'hints\' as NULL.");
        for (Hint hint : hints) {
            checkArgument(hint != null, "You cannot add NULL as a hint.");
        }
        this.name = name;
        this.type = type;
        this.sequence = sequence;
        this.defaultValue = defaultValueExpression;
        this.hints = Sets.newHashSet(hints);
        this.incomingForeignKeys = Lists.newArrayList();
    }

    public void addHint(Hint hint) {
        hints.add(hint);
    }

    @Override
    public Column copy() {
        return new Column(name, type, sequence, defaultValue, hints.stream().toArray(Hint[]::new));
    }

    public void dropDefaultValue() {
        this.defaultValue = null;
        this.sequence = null;
        this.hints.remove(Hint.AUTO_INCREMENT);
    }

    public void dropHint(Hint hint) {
        hints.remove(hint);
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Column))
            return false;
        final Column other = (Column) o;
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
        final java.lang.Object this$defaultValue = this.getDefaultValue();
        final java.lang.Object other$defaultValue = other.getDefaultValue();
        if (this$defaultValue == null ? other$defaultValue != null : !this$defaultValue.equals(other$defaultValue))
            return false;
        final java.lang.Object this$hints = this.getHints();
        final java.lang.Object other$hints = other.getHints();
        if (this$hints == null ? other$hints != null : !this$hints.equals(other$hints))
            return false;
        return true;
    }

    
    public String getDefaultValue() {
        return this.defaultValue;
    }

    
    public Set<Hint> getHints() {
        return this.hints;
    }

    
    public String getName() {
        return this.name;
    }

    
    public ForeignKey getOutgoingForeignKey() {
        return this.outgoingForeignKey;
    }

    
    public Table getParent() {
        return this.parent;
    }

    
    public Sequence getSequence() {
        return this.sequence;
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
        final java.lang.Object $defaultValue = this.getDefaultValue();
        result = result * PRIME + ($defaultValue == null ? 43 : $defaultValue.hashCode());
        final java.lang.Object $hints = this.getHints();
        result = result * PRIME + ($hints == null ? 43 : $hints.hashCode());
        return result;
    }

    public boolean isAutoIncrement() {
        return hints.contains(Hint.AUTO_INCREMENT);
    }

    public boolean isIdentity() {
        return hints.contains(Hint.IDENTITY);
    }

    public boolean isNotNull() {
        return hints.contains(Hint.NOT_NULL);
    }

    public void modifyDefaultValue(Sequence sequence) {
        this.hints.add(Hint.AUTO_INCREMENT);
        this.sequence = sequence;
        this.defaultValue = null;
    }

    public void modifyDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        this.sequence = null;
        this.hints.remove(Hint.AUTO_INCREMENT);
    }

    public void modifyType(ColumnType newColumnType) {
        this.type = newColumnType;
    }

    public Column rename(String newName) {
        checkArgument(!Strings.isNullOrEmpty(newName), "You must specify a \'name\'.");
        if (parent != null) {
            checkArgument(!parent.containsColumn(newName),
                          "Table: " + parent.getName() + " already contains column with name: " + newName);
        }
        this.name = newName;
        return this;
    }

    @Override
    public String toString() {
        return PrettyPrinter.prettyPrint(this);
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof Column;
    }

    
    protected List<ForeignKey> getIncomingForeignKeys() {
        return this.incomingForeignKeys;
    }

    
    protected void setOutgoingForeignKey(final ForeignKey outgoingForeignKey) {
        this.outgoingForeignKey = outgoingForeignKey;
    }

    
    protected void setSequence(final Sequence sequence) {
        this.sequence = sequence;
    }

    void setParent(Table parent) {
        this.parent = parent;
    }
}
