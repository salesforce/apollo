/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.dagwood.schema.Tables.CLOSURE;
import static com.salesforce.apollo.dagwood.schema.Tables.CONFLICTSET;
import static com.salesforce.apollo.dagwood.schema.Tables.DAG;
import static com.salesforce.apollo.dagwood.schema.Tables.LINK;
import static com.salesforce.apollo.dagwood.schema.Tables.UNQUERIED;
import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.mutNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.impl.DSL;

import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.dagwood.schema.tables.records.DagRecord;
import com.salesforce.apollo.protocols.HashKey;

import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.attribute.RankDir;
import guru.nidi.graphviz.attribute.Shape;
import guru.nidi.graphviz.attribute.Style;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.MutableNode;

/**
 * @author hal.hildebrand
 * @since 222
 */
public class DagViz {

    public static void dumpClosure(List<HashKey> nodes, DSLContext create) {
        nodes.forEach(k -> {
            System.out.println();
            System.out.println(String.format("%s : %s", k,
                                             create.select(DAG.CONFIDENCE)
                                                   .from(DAG)
                                                   .where(DAG.HASH.eq(k.bytes()))
                                                   .fetchOne()
                                                   .value1()));
            create.select(CLOSURE.CHILD, CLOSURE.DEPTH)
                  .from(CLOSURE)
                  .where(CLOSURE.PARENT.eq(DSL.inline(k.bytes())))
                  .and(CLOSURE.DEPTH.notEqual(DSL.inline(0)))
                  .orderBy(CLOSURE.DEPTH)
                  .stream()
                  .forEach(r -> {
                      System.out.println(String.format("   -> %s: %s", r.value2(), new HashKey(r.value1())));
                  });
        });
    }

    public static void dumpClosures(List<HASH> nodes, DSLContext create) {
        dumpClosure(nodes.stream().map(e -> new HashKey(e)).collect(Collectors.toList()), create);

    }

    public static MutableGraph visualize(HashKey node, String title, DSLContext create, boolean ignoreNoOp) {
        return mutGraph(title).setDirected(true).use((gr, ctx) -> {
            traverse(create, Collections.singletonList(node), ignoreNoOp);
        }).graphAttrs().add(RankDir.BOTTOM_TO_TOP);
    }

    public static MutableGraph visualize(String title, DSLContext create, boolean ignoreNoOp) {
        return mutGraph(title).setDirected(true).use((gr, ctx) -> {
            traverse(create, rawFrontier(create), ignoreNoOp);
        }).graphAttrs().add(RankDir.BOTTOM_TO_TOP);
    }

    static List<HashKey> rawFrontier(DSLContext create) {
        return create.select(DAG.HASH)
                     .from(DAG)
                     .leftAntiJoin(LINK)
                     .on(LINK.HASH.eq(DAG.HASH))
                     .stream()
                     .map(r -> new HashKey(r.value1()))
                     .collect(Collectors.toList());
    }

    static void traverse(DSLContext create, List<HashKey> roots, boolean ignoreNoOp) {
        Set<HashKey> traversed = new ConcurrentSkipListSet<>();
        Set<HashKey> frontier = new ConcurrentSkipListSet<>();
        Set<HashKey> next = new ConcurrentSkipListSet<>();
        Map<HashKey, String> labels = new ConcurrentSkipListMap<>();
        Function<HashKey, String> labelFor = h -> labels.computeIfAbsent(h,
                                                                         k -> k.b64Encoded().substring(0, 6));
        frontier.addAll(roots);

        while (!frontier.isEmpty()) {
            frontier.forEach(h -> {
                traversed.add(h);
                DagRecord entry = create.selectFrom(DAG).where(DAG.HASH.eq(h.bytes())).fetchOne();
                Result<Record1<byte[]>> links = null;
                if (entry != null) {
                    links = create.select(LINK.HASH)
                                  .from(LINK)
                                  .where(LINK.NODE.eq(h.bytes()))
                                  .fetch();
                }
                decorate(create, h, entry, labelFor, links, traversed, ignoreNoOp, next);
            });
            frontier.clear();
            frontier.addAll(next);
            next.clear();
        }
    }

    private static void decorate(DSLContext create, HashKey h, DagRecord entry,
            Function<HashKey, String> labelFor, Result<Record1<byte[]>> links, Set<HashKey> traversed,
            boolean ignoreNoOps, Set<HashKey> next) {
        String name = labelFor.apply(h);

        if (entry == null) {
            System.out.println("Missing from local DAG: " + h);
            MutableNode parent = mutNode(name);
            parent.add(Color.ORANGE);
            parent.add(Shape.OCTAGON);
            return;
        }

        links
             .forEach(c -> {
                 HashKey key = new HashKey(c.value1());
                 if (traversed.add(key)) {
                     next.add(key);
                 }
             });

        MutableNode parent;
        if (entry.getNoop()) {
            if (ignoreNoOps) { return; }
            parent = mutNode(name);
            parent.add(Color.RED);
        } else {
            parent = mutNode(name);
            parent.add(Color.BLUE);
        }

        parent.add(create.fetchExists(create.selectFrom(UNQUERIED).where(UNQUERIED.HASH.eq(h.bytes()))) ? Shape.DIAMOND
                : Shape.CIRCLE);
        if (!entry.getFinalized()) {
            parent.add(Style.DASHED);
        }

        links.forEach(c -> parent.addLink(mutNode(labelFor.apply(new HashKey(c.value1())))));

        Record4<Integer, byte[], Integer, Integer> info = create.select(CONFLICTSET.CARDINALITY, CONFLICTSET.PREFERRED,
                                                                        CONFLICTSET.COUNTER, UNQUERIED.TARGETROUND)
                                                                .from(CONFLICTSET)
                                                                .join(DAG)
                                                                .on(DAG.CONFLICTSET.eq(CONFLICTSET.NODE))
                                                                .join(UNQUERIED)
                                                                .on(UNQUERIED.HASH.eq(DAG.HASH))
                                                                .where(DAG.HASH.eq(h.bytes()))
                                                                .fetchOne();
        parent.add(Label.of(String.format("%s\n%s : %s : %s\n%s : %s : %s", name, entry.getChit(),
                                          entry.getConfidence(), info.value4(), info.value1(),
                                          info.value3(),
                                          Arrays.equals(info.value2(), h.bytes()))));
    }
}
