
package io.quantumdb.core.schema.definitions;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ColumnType {

    public static enum Type {
        BIGINT, BOOLEAN, CHAR, DATE, DOUBLE, FLOAT, INTEGER, OID, SMALLINT, TEXT, TIMESTAMP, UUID, VARCHAR, SERIAL, BIG_SERIAL;
    }

    @FunctionalInterface
    public interface ValueGenerator {
        Object generateValue();
    }

    @FunctionalInterface
    public interface ValueSetter {
        void setValue(PreparedStatement resultSet, int position, Object value) throws SQLException;
    }

    private final String         notation;
    private final boolean        requireQuotes;
    private final Type           type;
    private final ValueGenerator valueGenerator;
    private final ValueSetter    valueSetter;

    
    public ColumnType(final Type type, final boolean requireQuotes, final String notation,
            final ValueGenerator valueGenerator, final ValueSetter valueSetter) {
        this.type = type;
        this.requireQuotes = requireQuotes;
        this.notation = notation;
        this.valueGenerator = valueGenerator;
        this.valueSetter = valueSetter;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ColumnType))
            return false;
        final ColumnType other = (ColumnType) o;
        if (!other.canEqual(this))
            return false;
        if (this.isRequireQuotes() != other.isRequireQuotes())
            return false;
        final java.lang.Object this$type = this.getType();
        final java.lang.Object other$type = other.getType();
        if (this$type == null ? other$type != null : !this$type.equals(other$type))
            return false;
        final java.lang.Object this$notation = this.getNotation();
        final java.lang.Object other$notation = other.getNotation();
        if (this$notation == null ? other$notation != null : !this$notation.equals(other$notation))
            return false;
        final java.lang.Object this$valueGenerator = this.getValueGenerator();
        final java.lang.Object other$valueGenerator = other.getValueGenerator();
        if (this$valueGenerator == null ? other$valueGenerator != null
                : !this$valueGenerator.equals(other$valueGenerator))
            return false;
        final java.lang.Object this$valueSetter = this.getValueSetter();
        final java.lang.Object other$valueSetter = other.getValueSetter();
        if (this$valueSetter == null ? other$valueSetter != null : !this$valueSetter.equals(other$valueSetter))
            return false;
        return true;
    }

    
    public String getNotation() {
        return this.notation;
    }

    
    public Type getType() {
        return this.type;
    }

    
    public ValueGenerator getValueGenerator() {
        return this.valueGenerator;
    }

    
    public ValueSetter getValueSetter() {
        return this.valueSetter;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        result = result * PRIME + (this.isRequireQuotes() ? 79 : 97);
        final java.lang.Object $type = this.getType();
        result = result * PRIME + ($type == null ? 43 : $type.hashCode());
        final java.lang.Object $notation = this.getNotation();
        result = result * PRIME + ($notation == null ? 43 : $notation.hashCode());
        final java.lang.Object $valueGenerator = this.getValueGenerator();
        result = result * PRIME + ($valueGenerator == null ? 43 : $valueGenerator.hashCode());
        final java.lang.Object $valueSetter = this.getValueSetter();
        result = result * PRIME + ($valueSetter == null ? 43 : $valueSetter.hashCode());
        return result;
    }

    
    public boolean isRequireQuotes() {
        return this.requireQuotes;
    }

    @Override
    public String toString() {
        return notation;
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof ColumnType;
    }
}
