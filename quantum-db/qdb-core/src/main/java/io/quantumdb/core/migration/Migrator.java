
package io.quantumdb.core.migration;

import static com.google.common.base.Preconditions.checkState;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.DatabaseMigrator;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.schema.operations.Operation.Type;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;

public class Migrator {
    public static class Stage {
        private final Version       parent;
        private final Type          type;
        private final List<Version> versions;

        
        public Stage(final Type type, final List<Version> versions, final Version parent) {
            this.type = type;
            this.versions = versions;
            this.parent = parent;
        }

        @java.lang.Override
        
        public boolean equals(final java.lang.Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Migrator.Stage))
                return false;
            final Migrator.Stage other = (Migrator.Stage) o;
            if (!other.canEqual(this))
                return false;
            final java.lang.Object this$type = this.getType();
            final java.lang.Object other$type = other.getType();
            if (this$type == null ? other$type != null : !this$type.equals(other$type))
                return false;
            final java.lang.Object this$versions = this.getVersions();
            final java.lang.Object other$versions = other.getVersions();
            if (this$versions == null ? other$versions != null : !this$versions.equals(other$versions))
                return false;
            final java.lang.Object this$parent = this.getParent();
            final java.lang.Object other$parent = other.getParent();
            if (this$parent == null ? other$parent != null : !this$parent.equals(other$parent))
                return false;
            return true;
        }

        public Version getFirst() {
            checkState(!versions.isEmpty(), "A Stage must contain at least one version!");
            return versions.get(0);
        }

        public Version getLast() {
            checkState(!versions.isEmpty(), "A Stage must contain at least one version!");
            return versions.get(versions.size() - 1);
        }

        public Version getParent() {
            return parent;
        }

        
        public Type getType() {
            return this.type;
        }

        public ImmutableList<Version> getVersions() {
            return ImmutableList.copyOf(versions);
        }

        @java.lang.Override
        
        public int hashCode() {
            final int PRIME = 59;
            int result = 1;
            final java.lang.Object $type = this.getType();
            result = result * PRIME + ($type == null ? 43 : $type.hashCode());
            final java.lang.Object $versions = this.getVersions();
            result = result * PRIME + ($versions == null ? 43 : $versions.hashCode());
            final java.lang.Object $parent = this.getParent();
            result = result * PRIME + ($parent == null ? 43 : $parent.hashCode());
            return result;
        }

        @java.lang.Override
        
        public java.lang.String toString() {
            return "Migrator.Stage(type=" + this.getType() + ", versions=" + this.getVersions() + ", parent="
                    + this.getParent() + ")";
        }

        
        protected boolean canEqual(final java.lang.Object other) {
            return other instanceof Migrator.Stage;
        }

        void addVersion(Version version) {
            checkState(versions.isEmpty() || versions.contains(version.getParent()),
                       "The specified Version\'s parent must be present in the Stage!");
            versions.add(version);
        }
    }

    
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Migrator.class);

    private final Backend backend;

    public Migrator(Backend backend) {
        this.backend = backend;
    }

    public void drop(String versionId) throws MigrationException {
        DatabaseMigrator migrator = backend.getMigrator();
        State state = loadState();
        Changelog changelog = state.getChangelog();
        Version version = changelog.getVersion(versionId);
        migrator.drop(state, version);
    }

    public void migrate(String sourceVersionId, String targetVersionId) throws MigrationException {
        log.info("Forking from version: {} to version: {}", sourceVersionId, targetVersionId);
        State state = loadState();
        Changelog changelog = state.getChangelog();
        Version from = changelog.getVersion(sourceVersionId);
        Version to = changelog.getVersion(targetVersionId);
        Set<String> origins = state.getRefLog().getVersions().stream().map(Version::getId).collect(Collectors.toSet());
        if (origins.isEmpty()) {
            origins.add(changelog.getRoot().getId());
        }
        if (!origins.contains(sourceVersionId)) {
            log.warn("Not forking database, since we\'re not currently at version: {}", sourceVersionId);
            return;
        }
        if (origins.contains(targetVersionId)) {
            log.warn("Not forking database, since we\'re already at version: {}", targetVersionId);
            return;
        }
        DatabaseMigrator migrator = backend.getMigrator();
        List<Stage> stages = VersionTraverser.verifyPathAndState(state, from, to);
        Version intermediate = null;
        for (Stage stage : stages) {
            if (stage.getType() == Type.DDL) {
                log.info("Creating new state: {}", stage.getLast());
                migrator.applySchemaChanges(state, stage.getParent(), stage.getLast());
                if (intermediate != null) {
                    log.info("Dropping intermediate state: {}", intermediate.getId());
                    migrator.drop(state, intermediate);
                }
                intermediate = stage.getLast();
            } else if (stage.getType() == Type.DML) {
                log.info("Executing data changes: {}", stage.getVersions());
                migrator.applyDataChanges(state, stage);
            }
        }
    }

    private State loadState() throws MigrationException {
        try {
            return backend.loadState();
        } catch (SQLException e) {
            throw new MigrationException("Could not load current state: " + e.getMessage(), e);
        }
    }
}
