/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.mutNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.avalanche.WorkingSet.KnownNode;
import com.salesforce.apollo.avalanche.WorkingSet.Node;
import com.salesforce.apollo.protocols.HashKey;

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
        public final HashKey  key;
        public final DagEntry value;

        private KeyValue(HashKey key, DagEntry value) {
            super();
            this.key = key;
            this.value = value;
        }
    }

    public static void traverseClosure(DagEntry entry, WorkingSet dag, BiConsumer<HashKey, DagEntry> p) {
        Stack<DagEntry> stack = new Stack<>();
        stack.push(entry);
        Set<HashKey> visited = new TreeSet<>();

        while (!stack.isEmpty()) {
            final DagEntry node = stack.pop();
            final List<ByteString> links = node.getLinksList() == null ? Collections.emptyList() : node.getLinksList();
            for (HashKey e : links.stream().map(e -> new HashKey(e)).collect(Collectors.toList())) {
                if (visited.add(e)) {
                    DagEntry child = dag.getDagEntry(e);
                    p.accept(e, child);
                    stack.push(child);
                }
            }
        }
    }

    public static void dumpClosure(List<HashKey> nodes, WorkingSet dag) {
        nodes.stream().map(e -> new KeyValue(e, dag.getDagEntry(e))).filter(e -> e.value != null).forEach(n -> {
            System.out.println();
            System.out.println(String.format("%s :", n.key.b64Encoded()));
            traverseClosure(n.value, dag, (key, node) -> {
                System.out.println(String.format("   -> %s", key.b64Encoded()));
            });
        });
    }

    public static void dumpClosures(List<HashKey> nodes, WorkingSet dag) {
        dumpClosure(nodes, dag);

    }

    public static MutableGraph visualize(String title, WorkingSet dag, boolean ignoreNoOp) {
        return mutGraph(title).setDirected(true).use((gr, ctx) -> {
            traverse(dag, ignoreNoOp);
        });
    }

    static void traverse(WorkingSet dag, boolean ignoreNoOp) {
        Map<HashKey, String> labels = new ConcurrentSkipListMap<>();
        Function<HashKey, String> labelFor = h -> labels.computeIfAbsent(h, k -> k.b64Encoded());

        dag.traverseAll((k, e) -> {
            if (!(ignoreNoOp && e.getDescription() == null)) {
                decorate(k, e, labelFor,
                         e.getLinksList() == null ? Collections.emptyList()
                                 : e.getLinksList().stream().map(l -> new HashKey(l)).collect(Collectors.toList()),
                         dag);
            }
        });

    }

    private static MutableNode decorate(HashKey h, DagEntry entry, Function<HashKey, String> labelFor,
                                        List<HashKey> links, WorkingSet dag) {
        String name = labelFor.apply(h);
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

        links.forEach(c -> parent.addLink(labelFor.apply(c)));

        Node n = dag.get(h);
        ConflictSet cs = n instanceof KnownNode ? n.getConflictSet() : null;

        if (n != null) {
            parent.add(Label.of(String.format("%s\n%s : %s\n%s : %s : %s", name, n.getChit(), n.getConfidence(),
                                              cs == null ? 0 : cs.getCardinality(), cs == null ? 0 : cs.getCounter(),
                                              cs == null ? "-" : cs.getPreferred() == n)));
        }
        return parent;
    }
}
