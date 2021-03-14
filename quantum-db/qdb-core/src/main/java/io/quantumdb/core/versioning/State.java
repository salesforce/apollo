
package io.quantumdb.core.versioning;

import io.quantumdb.core.schema.definitions.Catalog;

public class State {
    private final Catalog   catalog;
    private final Changelog changelog;
    private final RefLog    refLog;

    
    public State(final Catalog catalog, final RefLog refLog, final Changelog changelog) {
        this.catalog = catalog;
        this.refLog = refLog;
        this.changelog = changelog;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof State))
            return false;
        final State other = (State) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$catalog = this.getCatalog();
        final java.lang.Object other$catalog = other.getCatalog();
        if (this$catalog == null ? other$catalog != null : !this$catalog.equals(other$catalog))
            return false;
        final java.lang.Object this$refLog = this.getRefLog();
        final java.lang.Object other$refLog = other.getRefLog();
        if (this$refLog == null ? other$refLog != null : !this$refLog.equals(other$refLog))
            return false;
        final java.lang.Object this$changelog = this.getChangelog();
        final java.lang.Object other$changelog = other.getChangelog();
        if (this$changelog == null ? other$changelog != null : !this$changelog.equals(other$changelog))
            return false;
        return true;
    }

    
    public Catalog getCatalog() {
        return this.catalog;
    }

    
    public Changelog getChangelog() {
        return this.changelog;
    }

    
    public RefLog getRefLog() {
        return this.refLog;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $catalog = this.getCatalog();
        result = result * PRIME + ($catalog == null ? 43 : $catalog.hashCode());
        final java.lang.Object $refLog = this.getRefLog();
        result = result * PRIME + ($refLog == null ? 43 : $refLog.hashCode());
        final java.lang.Object $changelog = this.getChangelog();
        result = result * PRIME + ($changelog == null ? 43 : $changelog.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "State(catalog=" + this.getCatalog() + ", refLog=" + this.getRefLog() + ", changelog="
                + this.getChangelog() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof State;
    }
}
