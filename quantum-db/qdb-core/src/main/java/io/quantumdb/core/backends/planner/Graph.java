
package io.quantumdb.core.backends.planner;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;

public class Graph {

    public static class GraphResult {
        private final long        count;
        private final Set<String> tableNames;

        
        public GraphResult(final long count, final Set<String> tableNames) {
            this.count = count;
            this.tableNames = tableNames;
        }

        @java.lang.Override
        
        public boolean equals(final java.lang.Object o) {
            if (o == this)
                return true;
            if (!(o instanceof Graph.GraphResult))
                return false;
            final Graph.GraphResult other = (Graph.GraphResult) o;
            if (!other.canEqual(this))
                return false;
            if (this.getCount() != other.getCount())
                return false;
            final java.lang.Object this$tableNames = this.getTableNames();
            final java.lang.Object other$tableNames = other.getTableNames();
            if (this$tableNames == null ? other$tableNames != null : !this$tableNames.equals(other$tableNames))
                return false;
            return true;
        }

        
        public long getCount() {
            return this.count;
        }

        
        public Set<String> getTableNames() {
            return this.tableNames;
        }

        @java.lang.Override
        
        public int hashCode() {
            final int PRIME = 59;
            int result = 1;
            final long $count = this.getCount();
            result = result * PRIME + (int) ($count >>> 32 ^ $count);
            final java.lang.Object $tableNames = this.getTableNames();
            result = result * PRIME + ($tableNames == null ? 43 : $tableNames.hashCode());
            return result;
        }

        @java.lang.Override
        
        public java.lang.String toString() {
            return "Graph.GraphResult(count=" + this.getCount() + ", tableNames=" + this.getTableNames() + ")";
        }

        
        protected boolean canEqual(final java.lang.Object other) {
            return other instanceof Graph.GraphResult;
        }
    }

    public static Graph fromCatalog(Catalog catalog, Set<String> tableNames, Set<String> viewNames) {
        Graph graph = new Graph(catalog);
        for (String tableName : tableNames) {
            graph.nodes.put(tableName, new TableNode(tableName, Lists.newArrayList()));
        }
        for (String tableName : tableNames) {
            TableNode node = graph.nodes.get(tableName);
            Table table = catalog.getTable(tableName);
            table.getForeignKeys().forEach(foreignKey -> {
                String referredTableName = foreignKey.getReferredTableName();
                if (tableNames.contains(referredTableName)) {
                    node.getForeignKeys().add(foreignKey);
                }
            });
        }
        return graph;
    }

    private final Catalog                catalog;
    private final Map<String, TableNode> nodes;

    public Graph(Catalog catalog) {
        this.catalog = catalog;
        this.nodes = Maps.newHashMap();
    }

    public TableNode get(String tableName) {
        return nodes.get(tableName);
    }

    public Set<String> getRefIds() {
        return nodes.keySet();
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    public Optional<GraphResult> leastOutgoingForeignKeys(Set<String> refIds) {
        Map<String, Long> outgoingForeignKeys = nodes.entrySet()
                                                     .stream()
                                                     .filter(entry -> refIds.contains(entry.getKey()))
                                                     .collect(Collectors.toMap(Entry::getKey,
                                                                               entry -> entry.getValue()
                                                                                             .getForeignKeys()
                                                                                             .stream()
                                                                                             .filter(foreignKey -> !foreignKey.isSelfReferencing())
                                                                                             .filter(foreignKey -> refIds.contains(foreignKey.getReferredTableName()))
                                                                                             .map(ForeignKey::getReferredTable)
                                                                                             .distinct()
                                                                                             .count()));
        if (outgoingForeignKeys.isEmpty()) {
            return Optional.of(new GraphResult(0, refIds));
        }
        long minimum = outgoingForeignKeys.values().stream().reduce(Long.MAX_VALUE, Math::min);
        Set<String> tableNames = outgoingForeignKeys.entrySet()
                                                    .stream()
                                                    .filter(entry -> entry.getValue() == minimum)
                                                    .map(Entry::getKey)
                                                    .collect(Collectors.toSet());
        return Optional.of(new GraphResult(minimum, tableNames));
    }

    public Optional<GraphResult> mostIncomingForeignKeys(Set<String> refIds) {
        Map<String, Long> incomingForeignKeys = nodes.entrySet()
                                                     .stream()
                                                     .filter(entry -> refIds.contains(entry.getKey()))
                                                     .collect(Collectors.toMap(Entry::getKey, entry -> {
                                                         String tableName = entry.getKey();
                                                         return catalog.getForeignKeys()
                                                                       .stream()
                                                                       .filter(foreignKey -> foreignKey.getReferredTableName()
                                                                                                       .equals(tableName))
                                                                       .filter(ForeignKey::isNotNullable)
                                                                       .map(ForeignKey::getReferencingTableName)
                                                                       .distinct()
                                                                       .count();
                                                     }));
        if (incomingForeignKeys.isEmpty()) {
            return Optional.of(new GraphResult(0, refIds));
        }
        long maximum = incomingForeignKeys.values().stream().reduce(0L, Math::max);
        Set<String> tableNames = incomingForeignKeys.entrySet()
                                                    .stream()
                                                    .filter(entry -> entry.getValue() == maximum)
                                                    .map(Entry::getKey)
                                                    .collect(Collectors.toSet());
        return Optional.of(new GraphResult(maximum, tableNames));
    }

    public void remove(String tableName) {
        TableNode removed = nodes.remove(tableName);
        if (removed != null) {
            nodes.values().forEach(node -> {
                Set<ForeignKey> toRemoved = Sets.newHashSet();
                node.getForeignKeys().forEach(foreignKey -> {
                    if (tableName.equals(foreignKey.getReferredTableName())) {
                        toRemoved.add(foreignKey);
                    }
                });
                toRemoved.forEach(node.getForeignKeys()::remove);
            });
        }
    }
}
