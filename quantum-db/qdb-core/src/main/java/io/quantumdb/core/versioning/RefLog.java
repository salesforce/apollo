
package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.quantumdb.core.migration.VersionTraverser;
import io.quantumdb.core.migration.VersionTraverser.Direction;
import io.quantumdb.core.schema.definitions.Catalog;

public class RefLog {
    public static class ColumnRef {
        private final Set<ColumnRef> basedOn;
        private final Set<ColumnRef> basisFor;
        private String               name;
        private TableRef             table;

        public ColumnRef(String name) {
            this(name, Sets.newHashSet());
        }

        public ColumnRef(String name, Collection<ColumnRef> basedOn) {
            this.name = name;
            this.basedOn = Sets.newHashSet(basedOn);
            this.basisFor = Sets.newHashSet();
            basedOn.forEach(column -> column.basisFor.add(this));
        }

        public ColumnRef(String name, ColumnRef... basedOn) {
            this(name, Sets.newHashSet(basedOn));
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof ColumnRef) {
                ColumnRef otherRef = (ColumnRef) other;
                EqualsBuilder builder = new EqualsBuilder().append(name, otherRef.getName());
                if (table != null) {
                    builder.append(table.getName(), otherRef.getTable().getName());
                } else if (otherRef.getTable() != null) {
                    return false;
                }
                return builder.isEquals();
            }
            return false;
        }

        public ImmutableSet<ColumnRef> getBasedOn() {
            return ImmutableSet.copyOf(basedOn);
        }

        public ImmutableSet<ColumnRef> getBasisFor() {
            return ImmutableSet.copyOf(basisFor);
        }

        
        public String getName() {
            return this.name;
        }

        
        public TableRef getTable() {
            return this.table;
        }

        public ColumnRef ghost() {
            return new ColumnRef(name, Sets.newHashSet(this));
        }

        @Override
        public int hashCode() {
            HashCodeBuilder builder = new HashCodeBuilder().append(name);
            if (table != null) {
                builder.append(table.getName());
            }
            return builder.toHashCode();
        }

        
        public void setName(final String name) {
            this.name = name;
        }

        
        public void setTable(final TableRef table) {
            this.table = table;
        }

        @java.lang.Override
        
        public java.lang.String toString() {
            return "RefLog.ColumnRef(name=" + this.getName() + ")";
        }

        void drop() {
            for (ColumnRef from : basedOn) {
                for (ColumnRef to : basisFor) {
                    to.basedOn.remove(this);
                    to.basedOn.add(from);
                    from.basisFor.remove(this);
                    from.basisFor.add(to);
                }
            }
        }
    }

    public static abstract class DataRef {
        private String             name;
        private String             refId;
        private final RefLog       refLog;
        private final Set<Version> versions;

        private DataRef(RefLog refLog, String name, String refId, Version version) {
            this.name = name;
            this.refId = refId;
            this.refLog = refLog;
            this.versions = Sets.newHashSet();
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof DataRef) {
                DataRef otherRef = (DataRef) other;
                return new EqualsBuilder().append(name, otherRef.getName())
                                          .append(refId, otherRef.getRefId())
                                          .append(versions, otherRef.getVersions())
                                          .isEquals();
            }
            return false;
        }

        
        public String getName() {
            return this.name;
        }

        
        public String getRefId() {
            return this.refId;
        }

        
        public RefLog getRefLog() {
            return this.refLog;
        }

        
        public Set<Version> getVersions() {
            return this.versions;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(name).append(refId).toHashCode();
        }

        public DataRef rename(String newName) {
            this.name = newName;
            return this;
        }

        
        public void setName(final String name) {
            this.name = name;
        }

        
        public void setRefId(final String refId) {
            this.refId = refId;
        }

        @java.lang.Override
        
        public java.lang.String toString() {
            return "RefLog.DataRef(name=" + this.getName() + ", refId=" + this.getRefId() + ", versions="
                    + this.getVersions() + ")";
        }

        protected DataRef markAsAbsent(Version version) {
            getVersions().remove(version);
            getRefLog().refMapping.remove(version, this);
            log.debug("Marked TableRef: {} ({}) as absent in version: {}", getName(), getRefId(), version.getId());
            return this;
        }

        protected DataRef markAsPresent(Version version) {
            getVersions().add(version);
            getRefLog().refMapping.put(version, this);
            log.debug("Marked TableRef: {} ({}) as present in version: {}", getName(), getRefId(), version.getId());
            return this;
        }
    }

    public static class SyncRef {
        private final Map<ColumnRef, ColumnRef> columnMapping;
        private final String                    functionName;
        private final String                    name;
        private final TableRef                  source;
        private final TableRef                  target;

        private SyncRef(String name, String functionName, Map<ColumnRef, ColumnRef> columnMapping) {
            this.name = name;
            this.functionName = functionName;
            this.columnMapping = ImmutableMap.copyOf(columnMapping);
            List<TableRef> sources = columnMapping.keySet()
                                                  .stream()
                                                  .map(ColumnRef::getTable)
                                                  .distinct()
                                                  .collect(Collectors.toList());
            List<TableRef> targets = columnMapping.values()
                                                  .stream()
                                                  .map(ColumnRef::getTable)
                                                  .distinct()
                                                  .collect(Collectors.toList());
            checkArgument(sources.size() == 1, "There can be only one source table!");
            checkArgument(targets.size() == 1, "There can be only one target table!");
            this.source = sources.get(0);
            this.target = targets.get(0);
            checkArgument(!source.equals(target), "You cannot add a recursive sync function!");
            source.outboundSyncs.add(this);
            target.inboundSyncs.add(this);
            columnMapping.forEach((from, to) -> {
                from.basisFor.add(to);
                to.basedOn.add(from);
            });
        }

        public void addColumnMapping(ColumnRef source, ColumnRef target) {
            columnMapping.put(source, target);
        }

        public void drop() {
            source.outboundSyncs.remove(this);
            target.inboundSyncs.remove(this);
        }

        public void dropColumnMapping(ColumnRef source, ColumnRef target) {
            columnMapping.remove(source, target);
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof SyncRef) {
                SyncRef otherRef = (SyncRef) other;
                EqualsBuilder builder = new EqualsBuilder().append(name, otherRef.getName())
                                                           .append(functionName, otherRef.getFunctionName());
                if (source != null) {
                    builder.append(source.getName(), otherRef.getSource().getName());
                } else if (otherRef.getSource() != null) {
                    return false;
                }
                if (target != null) {
                    builder.append(target.getName(), otherRef.getTarget().getName());
                } else if (otherRef.getTarget() != null) {
                    return false;
                }
                return builder.isEquals();
            }
            return false;
        }

        public ImmutableMap<ColumnRef, ColumnRef> getColumnMapping() {
            return ImmutableMap.copyOf(columnMapping);
        }

        public Direction getDirection() {
            Version origin = VersionTraverser.getFirst(source.getVersions());
            Version destination = VersionTraverser.getFirst(target.getVersions());
            return VersionTraverser.getDirection(origin, destination);
        }

        
        public String getFunctionName() {
            return this.functionName;
        }

        
        public String getName() {
            return this.name;
        }

        
        public TableRef getSource() {
            return this.source;
        }

        
        public TableRef getTarget() {
            return this.target;
        }

        @Override
        public int hashCode() {
            HashCodeBuilder builder = new HashCodeBuilder().append(name).append(functionName);
            if (source != null) {
                builder.append(source.getName());
            }
            if (target != null) {
                builder.append(target.getName());
            }
            return builder.toHashCode();
        }

        @java.lang.Override
        
        public java.lang.String toString() {
            return "RefLog.SyncRef(name=" + this.getName() + ", functionName=" + this.getFunctionName()
                    + ", columnMapping=" + this.getColumnMapping() + ", source=" + this.getSource() + ", target="
                    + this.getTarget() + ")";
        }
    }

    public static class TableRef extends DataRef {
        private final Map<String, ColumnRef> columns;
        private final Set<SyncRef>           inboundSyncs;
        private final Set<SyncRef>           outboundSyncs;

        private TableRef(RefLog refLog, String name, String refId, Version version, Collection<ColumnRef> columns) {
            super(refLog, name, refId, version);
            this.columns = Maps.newLinkedHashMap();
            this.outboundSyncs = Sets.newHashSet();
            this.inboundSyncs = Sets.newHashSet();
            columns.forEach(column -> {
                column.table = this;
                this.columns.put(column.getName(), column);
            });
            markAsPresent(version);
        }

        public TableRef addColumn(ColumnRef column) {
            columns.put(column.getName(), column);
            column.setTable(this);
            return this;
        }

        public ColumnRef dropColumn(String name) {
            ColumnRef removed = columns.remove(name);
            removed.drop();
            return removed;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TableRef) {
                TableRef otherRef = (TableRef) other;
                return super.equals(other) && new EqualsBuilder().append(columns, otherRef.getColumns())
                                                                 .append(inboundSyncs, otherRef.getInboundSyncs())
                                                                 .append(outboundSyncs, otherRef.getOutboundSyncs())
                                                                 .isEquals();
            }
            return false;
        }

        public Set<TableRef> getBasedOn() {
            return columns.values()
                          .stream()
                          .flatMap(column -> column.getBasedOn().stream())
                          .map(ColumnRef::getTable)
                          .distinct()
                          .collect(Collectors.toSet());
        }

        public Set<TableRef> getBasisFor() {
            return columns.values()
                          .stream()
                          .flatMap(column -> column.getBasisFor().stream())
                          .map(ColumnRef::getTable)
                          .distinct()
                          .collect(Collectors.toSet());
        }

        public ColumnRef getColumn(String name) {
            return columns.get(name);
        }

        public ImmutableMap<String, ColumnRef> getColumns() {
            return ImmutableMap.copyOf(columns);
        }

        public ImmutableSet<SyncRef> getInboundSyncs() {
            return ImmutableSet.copyOf(inboundSyncs);
        }

        public ImmutableSet<SyncRef> getOutboundSyncs() {
            return ImmutableSet.copyOf(outboundSyncs);
        }

        public TableRef ghost(String newRefId, Version version) {
            Collection<ColumnRef> newColumns = columns.values()
                                                      .stream()
                                                      .map(ColumnRef::ghost)
                                                      .collect(Collectors.toList());
            markAsAbsent(version);
            return new TableRef(getRefLog(), getName(), newRefId, version, newColumns);
        }

        @Override
        public TableRef rename(String newTableName) {
            super.rename(newTableName);
            return this;
        }

        public TableRef renameColumn(String oldName, String newName) {
            checkState(columns.containsKey(oldName), "Table: " + getRefId() + " does not contain a column: " + oldName);
            checkState(!columns.containsKey(newName),
                       "Table: " + getRefId() + " already contains a column: " + newName);
            ColumnRef removed = columns.remove(oldName);
            removed.name = newName;
            columns.put(newName, removed);
            return this;
        }

        @java.lang.Override
        
        public java.lang.String toString() {
            return "RefLog.TableRef(super=" + super.toString() + ", columns=" + this.getColumns() + ")";
        }

        void drop() {
            columns.forEach((name, ref) -> ref.drop());
            inboundSyncs.forEach(syncRef -> syncRef.getSource().outboundSyncs.remove(syncRef));
            outboundSyncs.forEach(syncRef -> syncRef.getTarget().inboundSyncs.remove(syncRef));
        }
    }

    public static class ViewRef extends DataRef {
        private ViewRef(RefLog refLog, String name, String viewId, Version version) {
            super(refLog, name, viewId, version);
            markAsPresent(version);
        }

        public void drop() {
        }

        @java.lang.Override
        
        public boolean equals(final java.lang.Object o) {
            if (o == this)
                return true;
            if (!(o instanceof RefLog.ViewRef))
                return false;
            final RefLog.ViewRef other = (RefLog.ViewRef) o;
            if (!other.canEqual(this))
                return false;
            return true;
        }

        public ViewRef ghost(String newViewId, Version version) {
            markAsAbsent(version);
            return new ViewRef(getRefLog(), getName(), newViewId, version);
        }

        @java.lang.Override
        
        public int hashCode() {
            final int result = 1;
            return result;
        }

        @Override
        public ViewRef rename(String newViewName) {
            super.rename(newViewName);
            return this;
        }

        @java.lang.Override
        
        public java.lang.String toString() {
            return "RefLog.ViewRef(super=" + super.toString() + ")";
        }

        
        protected boolean canEqual(final java.lang.Object other) {
            return other instanceof RefLog.ViewRef;
        }
    }

    
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RefLog.class);

    public static RefLog init(Catalog catalog, Version version) {
        return new RefLog().bootstrap(catalog, version);
    }

    private final Set<Version>               activeVersions;
    private final Multimap<Version, DataRef> refMapping;

    /**
     * Creates a new RefLog object.
     */
    public RefLog() {
        this.refMapping = LinkedHashMultimap.create();
        this.activeVersions = Sets.newLinkedHashSet();
    }

    /**
     * Defines that there's a trigger and function which manage the synchronization
     * between two different refMapping in one particular direction.
     *
     * @param name         The name of the trigger.
     * @param functionName The name of the function.
     * @param columns      The column mapping from the source table, to the target
     *                     table.
     * @return The constructed SyncRef object.
     */
    public SyncRef addSync(String name, String functionName, Map<ColumnRef, ColumnRef> columns) {
        long matches = refMapping.values().stream().filter(table -> table.getName().equals(name)).count();
        if (matches > 0) {
            throw new IllegalArgumentException("A SyncRef with name: " + name + " is already present!");
        }
        return new SyncRef(name, functionName, columns);
    }

    /**
     * Creates a new TableRef for the specified table ID, name, and columns at the
     * specified version.
     *
     * @param name    The name of the table.
     * @param refId   The table ID of the table.
     * @param version The version at which this table exists.
     * @param columns The columns present in the table.
     * @return The constructed TableRef object.
     */
    public TableRef addTable(String name, String refId, Version version, Collection<ColumnRef> columns) {
        checkArgument(!isNullOrEmpty(name), "You must specify a \'name\'!");
        checkArgument(!isNullOrEmpty(refId), "You must specify a \'refId\'!");
        checkArgument(version != null, "You must specify a \'version\'!");
        checkArgument(columns != null, "You must specify a collection of \'columns\'!");
        long matches = refMapping.get(version).stream().filter(table -> table.getName().equals(name)).count();
        if (matches > 0) {
            throw new IllegalStateException(
                    "A TableRef for tableName: " + name + " is already present for version: " + version.getId());
        }
        return new TableRef(this, name, refId, version, columns);
    }

    /**
     * Creates a new TableRef for the specified table ID, name, and columns at the
     * specified version.
     *
     * @param name    The name of the table.
     * @param refId   The table ID of the table.
     * @param version The version at which this table exists.
     * @param columns The columns present in the table.
     * @return The constructed TableRef object.
     */
    public TableRef addTable(String name, String refId, Version version, ColumnRef... columns) {
        return addTable(name, refId, version, Lists.newArrayList(columns));
    }

    /**
     * Creates a new ViewRef for the specified view ID, and name at the specified
     * version.
     *
     * @param name    The name of the view.
     * @param refId   The view ID of the view.
     * @param version The version at which this view exists.
     * @return The constructed ViewRef object.
     */
    public ViewRef addView(String name, String refId, Version version) {
        checkArgument(!isNullOrEmpty(name), "You must specify a \'name\'!");
        checkArgument(!isNullOrEmpty(refId), "You must specify a \'refId\'!");
        checkArgument(version != null, "You must specify a \'version\'!");
        long matches = refMapping.get(version).stream().filter(view -> view.getName().equals(name)).count();
        if (matches > 0) {
            throw new IllegalStateException(
                    "A ViewRef for viewName: " + name + " is already present for version: " + version.getId());
        }
        return new ViewRef(this, name, refId, version);
    }

    /**
     * Initializes this RefLog object based on the specified Catalog and current
     * Version.
     *
     * @param catalog The Catalog describing the current state of the database.
     * @param version The current version of the database.
     * @return The constructed RefLog object.
     */
    public RefLog bootstrap(Catalog catalog, Version version) {
        checkArgument(catalog != null, "You must specify a catalog!");
        checkArgument(version != null && version.getParent() == null, "You must specify a root version!");
        catalog.getTables().forEach(table -> {
            TableRef tableRef = addTable(table.getName(), table.getName(), version,
                                         table.getColumns()
                                              .stream()
                                              .map(column -> new ColumnRef(column.getName()))
                                              .collect(Collectors.toList()));
            log.debug("Added TableRef: {} (id: {}) for version: {} with columns: {}", table.getName(), table.getName(),
                      version, tableRef.getColumns().keySet());
        });
        // Register version as active.
        setVersionState(version, true);
        return this;
    }

    /**
     * Drops a TableRef entirely from the RefLog regardless of which versions it's
     * connected to.
     *
     * @param tableRef The TableRef object to drop from the RefLog.
     */
    public void dropTable(TableRef tableRef) {
        checkArgument(tableRef != null, "You must specify a TableRef!");
        List<Version> versions = refMapping.entries()
                                           .stream()
                                           .filter(entry -> entry.getValue().equals(tableRef))
                                           .map(Entry::getKey)
                                           .collect(Collectors.toList());
        versions.forEach(version -> refMapping.remove(version, tableRef));
        tableRef.drop();
    }

    /**
     * Drops a TableRef at a particular version, with a specific table name. If the
     * TableRef is only connected to the specified version, it will be disconnected
     * from the RefLog. If the TableRef is connected to multiple versions it will
     * remain connected to the RefLog.
     *
     * @param version   The version at which to remove the TableRef from the RefLog.
     * @param tableName The name of the table which the TableRef represents.
     * @return The dropped TableRef object.
     */
    public TableRef dropTable(Version version, String tableName) {
        checkArgument(version != null, "You must specify a version!");
        checkArgument(!isNullOrEmpty(tableName), "You must specify a table name!");
        TableRef tableRef = getTableRef(version, tableName);
        tableRef.getVersions().remove(version);
        refMapping.remove(version, tableRef);
        if (tableRef.getVersions().isEmpty()) {
            tableRef.drop();
        }
        return tableRef;
    }

    /**
     * Drops a ViewRef at a particular version, with a specific view name. If the
     * ViewRef is only connected to the specified version, it will be disconnected
     * from the RefLog. If the ViewRef is connected to multiple versions it will
     * remain connected to the RefLog.
     *
     * @param version  The version at which to remove the ViewRef from the RefLog.
     * @param viewName The name of the view which the ViewRef represents.
     * @return The dropped ViewRef object.
     */
    public ViewRef dropView(Version version, String viewName) {
        checkArgument(version != null, "You must specify a version!");
        checkArgument(!isNullOrEmpty(viewName), "You must specify a view name!");
        ViewRef viewRef = getViewRef(version, viewName);
        viewRef.getVersions().remove(version);
        refMapping.remove(version, viewRef);
        if (viewRef.getVersions().isEmpty()) {
            viewRef.drop();
        }
        return viewRef;
    }

    /**
     * Drops a ViewRef entirely from the RefLog regardless of which versions it's
     * connected to.
     *
     * @param viewRef The ViewRef object to drop from the RefLog.
     */
    public void dropView(ViewRef viewRef) {
        checkArgument(viewRef != null, "You must specify a ViewRef!");
        List<Version> versions = refMapping.entries()
                                           .stream()
                                           .filter(entry -> entry.getValue().equals(viewRef))
                                           .map(Entry::getKey)
                                           .collect(Collectors.toList());
        versions.forEach(version -> refMapping.remove(version, viewRef));
        viewRef.drop();
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof RefLog))
            return false;
        final RefLog other = (RefLog) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$refMapping = this.refMapping;
        final java.lang.Object other$refMapping = other.refMapping;
        if (this$refMapping == null ? other$refMapping != null : !this$refMapping.equals(other$refMapping))
            return false;
        final java.lang.Object this$activeVersions = this.activeVersions;
        final java.lang.Object other$activeVersions = other.activeVersions;
        if (this$activeVersions == null ? other$activeVersions != null
                : !this$activeVersions.equals(other$activeVersions))
            return false;
        return true;
    }

    /**
     * Creates an internal fork of the RefLog. The fork will be based on the
     * specified version's parent, and contain all TableRef objects which are also
     * present in the specified version's parent.
     *
     * @param version The next Version to fork to.
     * @return The same RefLog instance.
     */
    public RefLog fork(Version version) {
        checkArgument(version != null, "You must specify a version!");
        checkArgument(version.getParent() != null, "You cannot fork to a root version!");
        Version parent = version.getParent();
        checkArgument(refMapping.isEmpty() || refMapping.keySet().contains(parent),
                      "You cannot fork to a version whose parent is not in the RefLog!");
        refMapping.get(parent).forEach(table -> table.markAsPresent(version));
        return this;
    }

    /**
     * This method returns a Map which defines the evolutionary relation between
     * ColumnRefs in the specified 'from' TableRef, to the specified 'to' TableRef.
     *
     * @param from The source TableRef.
     * @param to   The target TableRef.
     * @return The column mapping between the two TableRefs.
     */
    public Map<ColumnRef, ColumnRef> getColumnMapping(TableRef from, TableRef to) {
        boolean forwards = isForwards(from, to);
        Multimap<ColumnRef, ColumnRef> mapping = HashMultimap.create();
        from.getColumns().forEach((k, v) -> mapping.put(v, v));
        while (true) {
            Multimap<ColumnRef, ColumnRef> pending = HashMultimap.create();
            for (ColumnRef source : mapping.keySet()) {
                Collection<ColumnRef> targets = mapping.get(source);
                targets.stream().filter(target -> {
                    TableRef table = target.getTable();
                    return !table.equals(to);
                }).forEach(target -> pending.put(source, target));
            }
            if (pending.isEmpty()) {
                break;
            }
            pending.entries().forEach(entry -> {
                ColumnRef columnRef = entry.getValue();
                mapping.remove(entry.getKey(), columnRef);
                Set<ColumnRef> nextColumns = null;
                if (forwards) {
                    nextColumns = columnRef.getBasisFor();
                } else {
                    nextColumns = columnRef.getBasedOn();
                }
                if (!nextColumns.isEmpty()) {
                    mapping.putAll(entry.getKey(), nextColumns);
                }
            });
        }
        return mapping.entries().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    public Multimap<TableRef, TableRef> getTableMapping(Version from, Version to) {
        return getTableMapping(from, to, true);
    }

    /**
     * Returns a Multimap defining the evolutionary relation between TableRefs in
     * the specified 'from' version, to TableRefs in specified the 'to' version.
     *
     * @param from The starting version.
     * @param to   The final version.
     * @return The mapping between TableRefs between these two versions.
     */
    public Multimap<TableRef, TableRef> getTableMapping(Version from, Version to, boolean filterUnchanged) {
        Multimap<TableRef, TableRef> mapping = HashMultimap.create();
        getTableRefs(from).forEach(tableRef -> {
            Set<TableRef> targets = Sets.newHashSet();
            List<TableRef> toCheck = Lists.newLinkedList();
            toCheck.add(tableRef);
            while (!toCheck.isEmpty()) {
                TableRef pointer = toCheck.remove(0);
                if (pointer.getVersions().contains(to)) {
                    if (!filterUnchanged || !pointer.getRefId().equals(tableRef.getRefId())) {
                        targets.add(pointer);
                    }
                } else {
                    toCheck.addAll(pointer.getBasisFor());
                }
            }
            mapping.putAll(tableRef, targets);
        });
        return mapping;
    }

    /**
     * This method retrieves the TableRef object from the RefLog with the specified
     * table name at the specified version. If no such TableRef matches these
     * criteria an IllegalArgumentException will be thrown.
     *
     * @param version   The version in which the TableRef should be present.
     * @param tableName The name of the table represented by the TableRef.
     * @return The retrieved TableRef object.
     * @throws IllegalArgumentException When no TableRef matches the specified
     *                                  criteria.
     */
    public TableRef getTableRef(Version version, String tableName) {
        checkArgument(version != null, "You must specify a version!");
        checkArgument(!isNullOrEmpty(tableName), "You must specify a table name!");
        return refMapping.get(version)
                         .stream()
                         .filter(ref -> ref instanceof TableRef)
                         .map(ref -> (TableRef) ref)
                         .filter(table -> table.getName().equals(tableName))
                         .findFirst()
                         .orElseThrow(() -> new IllegalArgumentException("Version: " + version.getId()
                                 + " does not contain a TableRef with tableName: " + tableName));
    }

    /**
     * This method retrieves the TableRef object from the RefLog with the specified
     * table ID. If no TableRef matches the criteria an IllegalArgumentException
     * will be thrown.
     *
     * @param refId The ID of the table represented by the TableRef.
     * @return The retrieved TableRef object.
     * @throws IllegalArgumentException When no TableRef matches the specified
     *                                  criteria.
     */
    public TableRef getTableRefById(String refId) {
        checkArgument(!isNullOrEmpty(refId), "You must specify a table ID!");
        return refMapping.values()
                         .stream()
                         .filter(ref -> ref instanceof TableRef)
                         .map(ref -> (TableRef) ref)
                         .filter(table -> table.getRefId().equals(refId))
                         .findFirst()
                         .orElseThrow(() -> new IllegalArgumentException("No table with id: " + refId));
    }

    /**
     * @return a Collection of TableRef objects currently registered with this
     *         RefLog object.
     */
    public Collection<TableRef> getTableRefs() {
        return ImmutableSet.copyOf(refMapping.values()
                                             .stream()
                                             .filter(ref -> ref instanceof TableRef)
                                             .map(ref -> (TableRef) ref)
                                             .collect(Collectors.toSet()));
    }

    /**
     * @return a Collection of TableRef objects currently registered with this
     *         RefLog object that are present in the specified version.
     */
    public Collection<TableRef> getTableRefs(Version version) {
        checkArgument(version != null, "You must specify a version!");
        return ImmutableSet.copyOf(refMapping.get(version)
                                             .stream()
                                             .filter(ref -> ref instanceof TableRef)
                                             .map(ref -> (TableRef) ref)
                                             .collect(Collectors.toSet()));
    }

    /**
     * @return An ImmutableSet of Versions covered by this RefLog.
     */
    public ImmutableSet<Version> getVersions() {
        return ImmutableSet.copyOf(activeVersions);
    }

    /**
     * This method retrieves the ViewRef object from the RefLog with the specified
     * table name at the specified version. If no such ViewRef matches these
     * criteria an IllegalArgumentException will be thrown.
     *
     * @param version  The version in which the ViewRef should be present.
     * @param viewName The name of the view represented by the ViewRef.
     * @return The retrieved ViewRef object.
     * @throws IllegalArgumentException When no ViewRef matches the specified
     *                                  criteria.
     */
    public ViewRef getViewRef(Version version, String viewName) {
        checkArgument(version != null, "You must specify a version!");
        checkArgument(!isNullOrEmpty(viewName), "You must specify a view name!");
        return refMapping.get(version)
                         .stream()
                         .filter(ref -> ref instanceof ViewRef)
                         .map(ref -> (ViewRef) ref)
                         .filter(view -> view.getName().equals(viewName))
                         .findFirst()
                         .orElseThrow(() -> new IllegalArgumentException("Version: " + version.getId()
                                 + " does not contain a ViewRef with viewName: " + viewName));
    }

    /**
     * This method retrieves the ViewRef object from the RefLog with the specified
     * view ID. If no ViewRef matches the criteria an IllegalArgumentException will
     * be thrown.
     *
     * @param refId The ID of the view represented by the ViewRef.
     * @return The retrieved ViewRef object.
     * @throws IllegalArgumentException When no ViewRef matches the specified
     *                                  criteria.
     */
    public ViewRef getViewRefById(String refId) {
        checkArgument(!isNullOrEmpty(refId), "You must specify a view ID!");
        return refMapping.values()
                         .stream()
                         .filter(ref -> ref instanceof ViewRef)
                         .map(ref -> (ViewRef) ref)
                         .filter(view -> view.getRefId().equals(refId))
                         .findFirst()
                         .orElseThrow(() -> new IllegalArgumentException("No view with id: " + refId));
    }

    /**
     * @return a Collection of ViewRef objects currently registered with this RefLog
     *         object.
     */
    public Collection<ViewRef> getViewRefs() {
        return ImmutableSet.copyOf(refMapping.values()
                                             .stream()
                                             .filter(ref -> ref instanceof ViewRef)
                                             .map(ref -> (ViewRef) ref)
                                             .collect(Collectors.toSet()));
    }

    /**
     * @return a Collection of ViewRef objects currently registered with this RefLog
     *         object that are present in the specified version.
     */
    public Collection<ViewRef> getViewRefs(Version version) {
        checkArgument(version != null, "You must specify a version!");
        return ImmutableSet.copyOf(refMapping.get(version)
                                             .stream()
                                             .filter(ref -> ref instanceof ViewRef)
                                             .map(ref -> (ViewRef) ref)
                                             .collect(Collectors.toSet()));
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $refMapping = this.refMapping;
        result = result * PRIME + ($refMapping == null ? 43 : $refMapping.hashCode());
        final java.lang.Object $activeVersions = this.activeVersions;
        result = result * PRIME + ($activeVersions == null ? 43 : $activeVersions.hashCode());
        return result;
    }

    /**
     * This method replaces an existing TableRef specified through the version, and
     * source table name, drops that TableRef for that particular version, and
     * creates a new TableRef with the new target table name, and table ID for that
     * particular version. The columns of the new TableRef will be based off the old
     * TableRef.
     *
     * @param version         The version at which the replace takes place.
     * @param sourceTableName The table name of the TableRef to replace.
     * @param targetTableName The table name of the TableRef which will replace the
     *                        old TableRef.
     * @param refId           The table ID of the TableRef which will replace the
     *                        old TableRef.
     * @return The created TableRef object.
     */
    public TableRef replaceTable(Version version, String sourceTableName, String targetTableName, String refId) {
        checkArgument(version != null, "You must specify a version!");
        checkArgument(!isNullOrEmpty(sourceTableName), "You must specify a source table name!");
        checkArgument(!isNullOrEmpty(targetTableName), "You must specify a target table name!");
        checkArgument(!isNullOrEmpty(refId), "You must specify a table ID!");
        TableRef tableRef = dropTable(version, sourceTableName);
        return new TableRef(this, targetTableName, refId, version,
                tableRef.getColumns()
                        .values()
                        .stream()
                        .map(columnRef -> new ColumnRef(columnRef.getName(), Lists.newArrayList(columnRef)))
                        .collect(Collectors.toList()));
    }

    /**
     * This method replaces an existing ViewRef specified through the version, and
     * source view name, drops that ViewRef for that particular version, and creates
     * a new ViewRef with the new target view name, and view ID for that particular
     * version.
     *
     * @param version        The version at which the replace takes place.
     * @param sourceViewName The view name of the ViewRef to replace.
     * @param targetViewName The view name of the ViewRef which will replace the old
     *                       ViewRef.
     * @param refId          The view ID of the ViewRef which will replace the old
     *                       ViewRef.
     * @return The created ViewRef object.
     */
    public ViewRef replaceView(Version version, String sourceViewName, String targetViewName, String refId) {
        checkArgument(version != null, "You must specify a version!");
        checkArgument(!isNullOrEmpty(sourceViewName), "You must specify a source view name!");
        checkArgument(!isNullOrEmpty(targetViewName), "You must specify a target view name!");
        checkArgument(!isNullOrEmpty(refId), "You must specify a view ID!");
        @SuppressWarnings("unused")
        ViewRef viewRef = dropView(version, sourceViewName);
        return new ViewRef(this, targetViewName, refId, version);
    }

    public void setVersionState(Version version, boolean active) {
        if (active) {
            activeVersions.add(version);
        } else {
            activeVersions.remove(version);
        }
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "RefLog(refMapping=" + this.refMapping + ", activeVersions=" + this.activeVersions + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof RefLog;
    }

    private boolean isForwards(TableRef from, TableRef to) {
        boolean forwards = false;
        if (!from.equals(to)) {
            Version origin = VersionTraverser.getFirst(from.getVersions());
            Version target = VersionTraverser.getFirst(to.getVersions());
            forwards = VersionTraverser.getDirection(origin, target) == Direction.FORWARDS;
        }
        return forwards;
    }
}
