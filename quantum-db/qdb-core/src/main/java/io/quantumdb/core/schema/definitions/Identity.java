
package io.quantumdb.core.schema.definitions;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import com.google.common.collect.Maps;

public class Identity {
    private final Map<String, Object> values;

    public Identity() {
        this.values = Maps.newLinkedHashMap();
    }

    public Identity(String key, Object value) {
        this();
        add(key, value);
    }

    public Identity add(String key, Object value) {
        values.put(key, value);
        return this;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Identity))
            return false;
        final Identity other = (Identity) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$values = this.values;
        final java.lang.Object other$values = other.values;
        if (this$values == null ? other$values != null : !this$values.equals(other$values))
            return false;
        return true;
    }

    public Object getValue(String key) {
        return values.get(key);
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $values = this.values;
        result = result * PRIME + ($values == null ? 43 : $values.hashCode());
        return result;
    }

    public Set<String> keys() {
        return values.keySet();
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "Identity(values=" + this.values + ")";
    }

    public Set<Entry<String, Object>> values() {
        return values.entrySet();
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof Identity;
    }
}
