/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.mutNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.avalanche.WorkingSet.KnownNode;
import com.salesforce.apollo.avalanche.WorkingSet.Node;
import com.salesforce.apollo.crypto.Digest;

import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.attribute.Shape;
import guru.nidi.graphviz.attribute.Style;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;

/**
 * @author hal.hildebrand
 * @since 222
 */
public class DagViz {

    private static class KeyValue {
        public final Digest   key;
        public final DagEntry value;

        private KeyValue(Digest key, DagEntry value) {
            super();
            this.key = key;
            this.value = value;
        }
    }

    private static class N {
        private final List<Digest> children;
        private final Digest       id;
        private boolean            visited = false;

        private N(Digest id, List<Digest> children) {
            this.id = id;
            this.children = children;
        }

    }

    public static void dumpClosure(List<Digest> nodes, WorkingSet dag) {
        nodes.stream().map(e -> new KeyValue(e, dag.getDagEntry(e))).filter(e -> e.value != null).forEach(n -> {
            System.out.println();
            System.out.println(String.format("%s :", qb64(n.key)));
            traverseClosure(n.value, dag, (key, node) -> {
                System.out.println(String.format("   -> %s", qb64(key)));
            });
        });
    }

    public static void dumpClosures(List<Digest> nodes, WorkingSet dag) {
        dumpClosure(nodes, dag);

    }

    public static void topoSortRecurse(N n, Map<Digest, N> nodes, List<N> sorted) {
        n.visited = true;
        for (Digest c : n.children) {
            N v = nodes.get(c);
            if (v == null) {
                System.out.println("Invalid child: " + c + " from: " + n.id);
            } else {
                if (!v.visited) {
                    topoSortRecurse(v, nodes, sorted);
                }
            }
        }
        sorted.add(0, n);
    }

    public static void traverseClosure(DagEntry entry, WorkingSet dag, BiConsumer<Digest, DagEntry> p) {
        List<DagEntry> stack = new ArrayList<>();
        stack.add(entry);
        Set<Digest> visited = new TreeSet<>();

        while (!stack.isEmpty()) {
            final DagEntry node = stack.remove(stack.size() - 1);
            final List<String> links = node.getLinksList() == null ? Collections.emptyList() : node.getLinksList();
            for (Digest e : links.stream().map(e -> digest(e)).collect(Collectors.toList())) {
                if (visited.add(e)) {
                    DagEntry child = dag.getDagEntry(e);
                    p.accept(e, child);
                    stack.add(child);
                }
            }
        }
    }

    public static MutableGraph visualize(String title, WorkingSet dag, boolean ignoreNoOp) {
        return mutGraph(title).setDirected(true).use((gr, ctx) -> {
            traverse(dag, ignoreNoOp);
        });
    }

    static void traverse(WorkingSet dag, boolean ignoreNoOp) {
        List<N> sorted = topoSort(dag);
        System.out.println("sorted: " + sorted.size());
        for (N n : sorted) {
            DagEntry e = dag.getDagEntry(n.id);
            if (!(ignoreNoOp && e.getDescription() == null)) {
                decorate(n.id, e, e.getLinksList() == null ? Collections.emptyList()
                        : e.getLinksList().stream().map(l -> digest(l)).collect(Collectors.toList()), dag);
            }
        }
        System.out.println("decorated");
    }

    private static MutableNode decorate(Digest h, DagEntry entry, List<Digest> links, WorkingSet dag) {
        String name = h.toString();
        MutableNode parent;
        if (entry.getDescription() == null) {
            parent = mutNode(name);
            parent.add(Color.RED);
        } else {
            parent = mutNode(name);
            parent.add(Color.BLUE);
        }

        boolean unqueried = dag.getUnqueried().contains(h);
        parent.add(unqueried ? Shape.DIAMOND : Shape.CIRCLE);
        if (!dag.isFinalized(h)) {
            parent.add(Style.DASHED);
        }

        links.forEach(c -> parent.addLink(c.toString()));

        Node n = dag.get(h);
        ConflictSet cs = n instanceof KnownNode ? n.getConflictSet() : null;

        if (n != null) {
            parent.add(Label.of(String.format("%s\n%s : %s\n%s : %s : %s", name, n.getChit(), n.getConfidence(),
                                              cs == null ? 0 : cs.getCardinality(), cs == null ? 0 : cs.getCounter(),
                                              cs == null ? "-" : cs.getPreferred() == n)));
        }
        return parent;
    }

    public static List<N> topoSort(WorkingSet dag) {
        Map<Digest, N> nodes = new HashMap<>();
        dag.traverseAll((k, e) -> {
            nodes.put(k, new N(k, e.getLinksList() == null ? Collections.emptyList()
                    : e.getLinksList().stream().map(id -> digest(id)).collect(Collectors.toList())));
        });
        List<N> sorted = new LinkedList<>();
        for (N n : nodes.values()) {
            if (!n.visited) {
                topoSortRecurse(n, nodes, sorted);
            }
        }
        return sorted;
    }
}
