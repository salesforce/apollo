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
import static com.salesforce.apollo.dagwood.schema.Tables.UNFINALIZED;
import static com.salesforce.apollo.protocols.Conversion.serialize;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.apollo.avalanche.Dag.FinalizationData;
import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 222
 */
public class TransactionsTest {

    public static void dumpClosure(List<HashKey> nodes, DSLContext create) {
        nodes.forEach(k -> {
            System.out.println();
            Record2<Integer, Integer> node = create.select(UNFINALIZED.CHIT, UNFINALIZED.CONFIDENCE)
                                                   .from(UNFINALIZED)
                                                   .where(UNFINALIZED.HASH.eq(k.bytes()))
                                                   .fetchOne();
            System.out.println(String.format("%s : %s : %s", k, node.value1(), node.value2()));
            create.select(CLOSURE.CHILD)
                  .from(CLOSURE)
                  .where(CLOSURE.PARENT.eq(DSL.inline(k.bytes())))
                  .and(CLOSURE.CLOSURE_.isTrue())
                  .stream()
                  .forEach(r -> {
                      System.out.println(String.format("   -> %s", new HashKey(r.value1())));
                  });
        });
    }

    private String              connection_url;
    private Connection          connection;
    private DSLContext          create;
    private Dag                 dag;
    private AvalancheParameters parameters;
    private DagEntry            root;

    private HASH rootKey;

    @After
    public void after() {
        if (create != null) {
            create.close();
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    @Before
    public void before() throws SQLException {
        connection_url = "jdbc:h2:mem:test-" + (Math.random() * 100);
        Avalanche.loadSchema(connection_url);
        connection = DriverManager.getConnection(connection_url, "apollo", "");
        connection.setAutoCommit(false);
        ConnectionProvider provider = new DefaultConnectionProvider(connection);
        create = DSL.using(provider, SQLDialect.H2);
        create.deleteFrom(CLOSURE).execute();
        create.deleteFrom(CONFLICTSET).execute();
        create.deleteFrom(DAG).execute();
        parameters = new AvalancheParameters();
        dag = new Dag(parameters);
        root = new DagEntry();
        root.setData(ByteBuffer.wrap("Ye root".getBytes()));
        rootKey = dag.putDagEntry(root, serialize(root), null, create, false, 0);
        assertNotNull(rootKey);
    }

    @Test
    public void consecutiveCounter() throws Exception {
        int oldBeta1 = parameters.beta1;
        int oldBeta2 = parameters.beta2;
        try {
            parameters.beta1 = 100;
            parameters.beta2 = 20;
            List<HashKey> ordered = new ArrayList<>();
            Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
            stored.put(new HashKey(rootKey), root);
            ordered.add(new HashKey(rootKey));

            // create.update(DAG).set(DAG.FINALIZED,
            // true).where(DAG.HASH.eq(rootKey.bytes())).execute();

            HASH last = rootKey;
            HASH firstCommit = newDagEntry("1st commit", ordered, stored, Arrays.asList(last));
            last = firstCommit;
            HASH secondCommit = newDagEntry("2nd commit", ordered, stored, Arrays.asList(last));
            last = secondCommit;

            for (int i = 0; i < parameters.beta2; i++) {
                last = newDagEntry("entry: " + i, ordered, stored, Arrays.asList(last));
            }

            for (int i = ordered.size() - 1; i >= 3; i--) {
                dag.prefer(ordered.get(i).toHash(), create);
                create.transaction(config -> dag.tryFinalize(ordered.stream()
                                                                    .map(e -> e.bytes())
                                                                    .collect(Collectors.toList()),
                                                             DSL.using(config)));
            }

            assertEquals(parameters.beta2,
                         create.select(CONFLICTSET.COUNTER)
                               .from(CONFLICTSET)
                               .join(UNFINALIZED)
                               .on(UNFINALIZED.HASH.eq(CONFLICTSET.PREFERRED))
                               .and(UNFINALIZED.HASH.eq(firstCommit.bytes()))
                               .fetchOne()
                               .value1()
                               .intValue());
            assertEquals(parameters.beta2,
                         create.select(CONFLICTSET.COUNTER)
                               .from(CONFLICTSET)
                               .join(UNFINALIZED)
                               .on(UNFINALIZED.HASH.eq(CONFLICTSET.PREFERRED))
                               .and(UNFINALIZED.HASH.eq(secondCommit.bytes()))
                               .fetchOne()
                               .value1()
                               .intValue());
            assertFalse(dag.isFinalized(rootKey, create));
            assertFalse(dag.isFinalized(firstCommit, create));
            assertFalse(dag.isFinalized(secondCommit, create));

            dag.prefer(ordered.get(ordered.size() - 1).toHash(), create);
            create.transactionResult(config -> dag.tryFinalize(ordered.stream()
                                                                      .map(e -> e.bytes())
                                                                      .collect(Collectors.toList()),
                                                               DSL.using(config)));
            assertTrue(dag.isFinalized(rootKey, create));
            assertTrue(dag.isFinalized(firstCommit, create));
            assertTrue(dag.isFinalized(secondCommit, create));
        } finally {
            parameters.beta1 = oldBeta1;
            parameters.beta2 = oldBeta2;
        }
    }

    // test early commit logic
    @Test
    public void earlyCommit() throws Exception {
        List<HashKey> ordered = new ArrayList<>();
        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(new HashKey(rootKey), root);
        ordered.add(new HashKey(rootKey));

        // create.update(DAG).set(DAG.FINALIZED,
        // true).where(DAG.HASH.eq(rootKey.bytes())).execute();

        HASH last = rootKey;
        HASH firstCommit = newDagEntry("1st commit", ordered, stored, Arrays.asList(last));
        last = firstCommit;
        HASH secondCommit = newDagEntry("2nd commit", ordered, stored, Arrays.asList(last));
        last = secondCommit;

        for (int i = 0; i < parameters.beta1; i++) {
            last = newDagEntry("entry: " + i, ordered, stored, Arrays.asList(last));
        }

        for (int i = ordered.size() - 1; i >= 3; i--) {
            dag.prefer(ordered.get(i).toHash(), create);
            create.transactionResult(config -> dag.tryFinalize(ordered.stream()
                                                                      .map(e -> e.bytes())
                                                                      .collect(Collectors.toList()),
                                                               DSL.using(config)));

            assertFalse(dag.isFinalized(firstCommit, create));
            assertFalse(dag.isFinalized(secondCommit, create));
        }

        assertEquals(parameters.beta1,
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(firstCommit.bytes()))
                           .fetchOne()
                           .value1()
                           .intValue());

        assertEquals(parameters.beta1,
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(secondCommit.bytes()))
                           .fetchOne()
                           .value1()
                           .intValue());

        dag.prefer(ordered.get(3).toHash(), create);
        create.transactionResult(config -> dag.tryFinalize(ordered.stream()
                                                                  .map(e -> e.bytes())
                                                                  .collect(Collectors.toList()),
                                                           DSL.using(config)));

        assertTrue(dag.isFinalized(rootKey, create));
        assertTrue(dag.isFinalized(firstCommit, create));
        assertTrue(dag.isFinalized(secondCommit, create));
    }

    @Test
    public void finalizedSet() {
        List<HashKey> ordered = new ArrayList<>();
        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(new HashKey(rootKey), root);
        ordered.add(new HashKey(rootKey));

        HASH last = rootKey;
        HASH firstCommit = newDagEntry("1st commit", ordered, stored, Arrays.asList(last));
        last = firstCommit;
        HASH secondCommit = newDagEntry("2nd commit", ordered, stored, Arrays.asList(last));
        last = secondCommit;

        for (int i = 0; i < parameters.beta2; i++) {
            last = newDagEntry("entry: " + i, ordered, stored, Arrays.asList(last));
        }
        FinalizationData finalized;

        for (int i = ordered.size() - 1; i >= ordered.size() - 11; i--) {
            dag.prefer(ordered.get(i).toHash(), create);
            finalized = create.transactionResult(config -> dag.tryFinalize(ordered.stream()
                                                                                  .map(e -> e.bytes())
                                                                                  .collect(Collectors.toList()),
                                                                           DSL.using(config)));
            assertEquals(0, finalized.finalized.size());
            assertEquals(0, finalized.deleted.size());
        }

        dag.prefer(ordered.get(ordered.size() - 1).toHash(), create);

        finalized = create.transactionResult(config -> dag.tryFinalize(ordered.stream()
                                                                              .map(e -> e.bytes())
                                                                              .collect(Collectors.toList()),
                                                                       DSL.using(config)));
        assertNotNull(finalized);
        assertEquals(143, finalized.finalized.size());
        assertEquals(0, finalized.deleted.size());
        for (HashKey key : finalized.finalized) {
            assertTrue("not finalized: " + key, dag.isFinalized(key.toHash(), create));
        }
    }

    @Test
    public void frontier() throws Exception {
        List<HashKey> ordered = new ArrayList<>();
        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(new HashKey(rootKey), root);
        ordered.add(new HashKey(rootKey));

        HashKey last = new HashKey(rootKey);
        HashKey firstCommit = new HashKey(newDagEntry("1st commit", ordered, stored, Arrays.asList(rootKey)));
        ordered.add(new HashKey(firstCommit.bytes()));
        last = firstCommit;

        HashKey secondCommit = new HashKey(newDagEntry("2nd commit", ordered, stored, Arrays.asList(rootKey)));
        ordered.add(new HashKey(secondCommit.bytes()));
        last = secondCommit;

        TreeSet<HashKey> frontier = dag.getNeglectedFrontier(create)
                                       .map(e -> new HashKey(e))
                                       .collect(Collectors.toCollection(TreeSet::new));

        assertEquals(3, frontier.size());

        assertTrue(frontier.contains(secondCommit));

        HashKey userTxn = new HashKey(newDagEntry("Ye test transaction", ordered, stored,
                                                  dag.sampleParents(create).stream().collect(Collectors.toList())));
        ordered.add(new HashKey(userTxn.bytes()));

        frontier = dag.getNeglectedFrontier(create)
                      .map(e -> new HashKey(e))
                      .collect(Collectors.toCollection(TreeSet::new));

        assertEquals(4, frontier.size());

        assertTrue(frontier.contains(secondCommit) || frontier.contains(firstCommit));
        assertTrue(frontier.contains(userTxn));

        last = userTxn;
        last = new HashKey(newDagEntree("entry: " + 0, ordered, stored, Arrays.asList(last)));

        frontier = dag.getNeglectedFrontier(create)
                      .map(e -> new HashKey(e))
                      .collect(Collectors.toCollection(TreeSet::new));

        assertEquals(5, frontier.size());

        assertTrue(frontier.contains(secondCommit) || frontier.contains(firstCommit));
        assertTrue(frontier.contains(last));
    }

    @Test
    public void isStronglyPreferred() throws Exception {
        List<HashKey> ordered = new ArrayList<>();
        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(new HashKey(rootKey), root);
        ordered.add(new HashKey(rootKey));

        DagEntry entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 1).getBytes()));
        entry.setLinks(asList(rootKey));
        HASH key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 2).getBytes()));
        entry.setLinks(asList(key));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 3).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(2).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        HASH zero = new HASH(new byte[32]);
        assertNull("Not exist returned true: ", dag.isStronglyPreferred(zero, create));

        byte[] o = new byte[32];
        Arrays.fill(o, (byte) 1);
        HASH one = new HASH(o);
        assertNull("Not exist returned true: ",
                   create.transactionResult(config -> dag.isStronglyPreferred(one, DSL.using(config))));
        assertArrayEquals("Aggregate failed: ", new Boolean[] { null, null },
                          dag.isStronglyPreferred(Arrays.asList(zero, one), create).toArray(new Boolean[2]));

        // All are strongly preferred
        for (int i = 0; i < ordered.size(); i++) {
            HASH test = ordered.get(i).toHash();
            assertTrue(String.format("node %s is not strongly preferred", i), create.transactionResult(config -> {
                return dag.isStronglyPreferred(test, DSL.using(config));
            }));
        }

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 4).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(2).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), ordered.get(3).toHash(), create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        assertFalse(String.format("node 4 is strongly preferred: " + ordered.get(4)),
                    dag.isStronglyPreferred(ordered.get(4).toHash(), create));

        for (int i = 0; i < 4; i++) {
            int it = i;
            assertTrue(String.format("node %s is not strongly preferred", i),
                       dag.isStronglyPreferred(ordered.get(it).toHash(), create));
        }

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 5).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(4).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        // check transitivity of isStronglyPreferred()
        assertFalse(String.format("node 5 is strongly preferred"),
                    create.transactionResult(config -> dag.isStronglyPreferred(ordered.get(5).toHash(),
                                                                               DSL.using(config))));

        Boolean[] expected = new Boolean[] { true, true, true, true, false, false };
        List<HASH> all = ordered.stream().map(e -> e.toHash()).collect(Collectors.toList());
        assertArrayEquals("Aggregate failed: ", expected,
                          dag.isStronglyPreferred(all, create).toArray(new Boolean[ordered.size()]));
        dag.finalize(rootKey, create);
        assertTrue(dag.isFinalized(rootKey, create));
        assertTrue(dag.isStronglyPreferred(rootKey, create));
        assertArrayEquals("Aggregate failed: ", expected,
                          dag.isStronglyPreferred(all, create).toArray(new Boolean[ordered.size()]));
    }

    @Test
    public void multipleParents() {
        List<HashKey> ordered = new ArrayList<>();
        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(new HashKey(rootKey), root);
        ordered.add(new HashKey(rootKey));

        HASH last = rootKey;
        HASH firstCommit = newDagEntry("1st commit", ordered, stored, Arrays.asList(last));
        last = firstCommit;
        HASH secondCommit = newDagEntry("2nd commit", ordered, stored, Arrays.asList(last));
        last = secondCommit;

        HASH userTxn = newDagEntry("Ye test transaction", ordered, stored,
                                   dag.sampleParents(create).stream().collect(Collectors.toList()));

        last = userTxn;

        for (int i = 0; i < parameters.beta2; i++) {
            last = newDagEntry("entry: " + i, ordered, stored, Arrays.asList(last));
        }

        for (int i = ordered.size() - 1; i >= 4; i--) {
            dag.prefer(ordered.get(i).toHash(), create);
        }

        dag.prefer(userTxn, create);
        create.transactionResult(config -> dag.tryFinalize(ordered.stream()
                                                                  .map(e -> e.bytes())
                                                                  .collect(Collectors.toList()),
                                                           DSL.using(config)));
        assertTrue(dag.isFinalized(userTxn, create));
    }

    @Test
    public void parentSelection() throws Exception {
        List<HashKey> ordered = new ArrayList<>();
        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(new HashKey(rootKey), root);
        ordered.add(new HashKey(rootKey));

        // Finalize ye root
        dag.finalize(rootKey, create);
        assertTrue(dag.isFinalized(rootKey, create));

        // 1 elegible parent, the root
        Set<HashKey> sampled = dag.sampleParents(create)
                                  .stream()
                                  .map(h -> new HashKey(h))
                                  .collect(Collectors.toCollection(ConcurrentSkipListSet::new));
        assertEquals(1, sampled.size());
        assertTrue(sampled.contains(ordered.get(0)));

        DagEntry entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 1).getBytes()));
        entry.setLinks(asList(rootKey));
        HASH key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        sampled = dag.sampleParents(create)
                     .stream()
                     .map(h -> new HashKey(h))
                     .collect(Collectors.toCollection(ConcurrentSkipListSet::new));
        assertEquals(1, sampled.size());
        assertTrue(sampled.contains(ordered.get(1)));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 2).getBytes()));
        entry.setLinks(asList(key));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        sampled = dag.sampleParents(create)
                     .stream()
                     .map(h -> new HashKey(h))
                     .collect(Collectors.toCollection(ConcurrentSkipListSet::new));
        assertEquals(2, sampled.size());
        assertTrue(sampled.contains(ordered.get(1)));
        assertTrue(sampled.contains(ordered.get(2)));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 3).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(2).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 4).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(2).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), ordered.get(3).toHash(), create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 5).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(3).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        sampled = dag.sampleParents(create)
                     .stream()
                     .map(h -> new HashKey(h))
                     .collect(Collectors.toCollection(ConcurrentSkipListSet::new));
        assertEquals(4, sampled.size());

        assertTrue(sampled.contains(ordered.get(1)));
        assertTrue(sampled.contains(ordered.get(2)));
        assertTrue(sampled.contains(ordered.get(3)));
        assertTrue(sampled.contains(ordered.get(5)));

        // Add a new node to the frontier
        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 6).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(5).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        sampled = dag.sampleParents(create)
                     .stream()
                     .map(h -> new HashKey(h))
                     .collect(Collectors.toCollection(ConcurrentSkipListSet::new));

        assertEquals(5, sampled.size());

        assertTrue(sampled.contains(ordered.get(1)));
        assertTrue(sampled.contains(ordered.get(2)));
        assertTrue(sampled.contains(ordered.get(3)));
        assertTrue(sampled.contains(ordered.get(5)));
        assertTrue(sampled.contains(ordered.get(6)));
    }

    @Test
    public void parentSelectionWithPreferred() throws Exception {
        List<HashKey> ordered = new ArrayList<>();
        Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();
        stored.put(new HashKey(rootKey), root);
        ordered.add(new HashKey(rootKey));

        DagEntry entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 1).getBytes()));
        entry.setLinks(asList(rootKey));
        HASH key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 2).getBytes()));
        entry.setLinks(asList(key));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 3).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(2).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 4).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(2).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), ordered.get(3).toHash(), create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 5).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(3).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        Set<HashKey> frontier = dag.frontierSample(create)
                                   .map(r -> new HashKey(r))
                                   .collect(Collectors.toCollection(TreeSet::new));
        assertEquals(5, frontier.size());

        // Nodes 3 and 4 are in conflict and are always excluded
        assertTrue(frontier.contains(ordered.get(3)));
        assertFalse(frontier.contains(ordered.get(4)));

        // Add a new node to the frontier
        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 6).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(5).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        frontier = dag.frontierSample(create).map(r -> new HashKey(r)).collect(Collectors.toCollection(TreeSet::new));

        assertEquals(6, frontier.size());

        // prefer node 6, raising the confidence of nodes 3, 2, 1 and 0
        dag.prefer(ordered.get(6).toHash(), create);
        frontier = dag.frontierSample(create).map(r -> new HashKey(r)).collect(Collectors.toCollection(TreeSet::new));

        assertEquals(6, frontier.size());

        assertTrue(frontier.contains(ordered.get(3)));
        assertFalse(frontier.contains(ordered.get(4)));
    }

    @Test
    public void prefer() throws Exception {
        List<HashKey> ordered = new ArrayList<>();
        Map<HashKey, DagEntry> stored = new TreeMap<>();
        HashKey rootHashKey = new HashKey(rootKey);
        stored.put(rootHashKey, root);
        ordered.add(rootHashKey);

        DagEntry entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 1).getBytes()));
        entry.setLinks(asList(rootKey));
        HASH key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 2).getBytes()));
        entry.setLinks(asList(key));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        // Nodes 3, 4 conflict. 3 is the preference initially
        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 3).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(2).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 4).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(2).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), ordered.get(3).toHash(), create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 5).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(4).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

//		dumpClosure(ordered, create);
        for (HashKey e : ordered) {
            assertEquals(Integer.valueOf(0),
                         create.select(UNFINALIZED.CONFIDENCE)
                               .from(UNFINALIZED)
                               .where(UNFINALIZED.HASH.eq(e.bytes()))
                               .fetchOne()
                               .value1());
//			assertFalse(String.format("node " + e + " is strongly preferred: ") + ordered.get(4),
//						dag.isStronglyPreferred(e.toHash(), create));
        }

        dag.prefer(ordered.get(3).toHash(), create);

//		dumpClosure(ordered, create);
        assertFalse(String.format("node 4 is strongly preferred: ") + ordered.get(4),
                    dag.isStronglyPreferred(ordered.get(4).toHash(), create));

        assertEquals(Integer.valueOf(1),
                     create.select(UNFINALIZED.CHIT)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(3).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(1),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(0).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(1),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(2).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(0),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(4).bytes()))
                           .fetchOne()
                           .value1());
        assertTrue(String.format("node 0 is not strongly preferred"),
                   dag.isStronglyPreferred(ordered.get(0).toHash(), create));
        assertTrue(String.format("node 1 is not strongly preferred"),
                   dag.isStronglyPreferred(ordered.get(1).toHash(), create));
        assertTrue(String.format("node 2 is not strongly preferred"),
                   dag.isStronglyPreferred(ordered.get(2).toHash(), create));
        assertTrue(String.format("node 3 is not strongly preferred"),
                   dag.isStronglyPreferred(ordered.get(3).toHash(), create));
        assertFalse(String.format("node 4 is strongly preferred"),
                    create.transactionResult(config -> dag.isStronglyPreferred(ordered.get(4).toHash(),
                                                                               DSL.using(config))));

        dag.prefer(ordered.get(4).toHash(), create);

        assertEquals(Integer.valueOf(1),
                     create.select(UNFINALIZED.CHIT)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(4).bytes()))
                           .fetchOne()
                           .value1());

        assertEquals(Integer.valueOf(2),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(0).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(2),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(2).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(1),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(3).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(1),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(4).bytes()))
                           .fetchOne()
                           .value1());

        assertTrue(String.format("node 3 is not strongly preferred " + ordered.get(3)),
                   create.transactionResult(config -> dag.isStronglyPreferred(ordered.get(3).toHash(),
                                                                              DSL.using(config))));
        assertFalse(String.format("node 4 is strongly preferred"),
                    create.transactionResult(config -> dag.isStronglyPreferred(ordered.get(4).toHash(),
                                                                               DSL.using(config))));

        entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 6).getBytes()));
        entry.setLinks(asList(ordered.get(1).toHash(), ordered.get(5).toHash()));
        key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));

        dag.prefer(ordered.get(5).toHash(), create);

        assertEquals(Integer.valueOf(1),
                     create.select(UNFINALIZED.CHIT)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(4).bytes()))
                           .fetchOne()
                           .value1());

        assertEquals(Integer.valueOf(3),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(0).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(3),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(2).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(1),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(3).bytes()))
                           .fetchOne()
                           .value1());
        assertEquals(Integer.valueOf(2),
                     create.select(UNFINALIZED.CONFIDENCE)
                           .from(UNFINALIZED)
                           .where(UNFINALIZED.HASH.eq(ordered.get(4).bytes()))
                           .fetchOne()
                           .value1());

        assertFalse(String.format("node 3 is strongly preferred " + ordered.get(3)),
                    create.transactionResult(config -> dag.isStronglyPreferred(ordered.get(3).toHash(),
                                                                               DSL.using(config))));
        assertTrue(String.format("node 4 is not strongly preferred"),
                   create.transactionResult(config -> dag.isStronglyPreferred(ordered.get(4).toHash(),
                                                                              DSL.using(config))));
    }

    HASH newDagEntree(String contents, List<HashKey> ordered, Map<HashKey, DagEntry> stored, List<HashKey> links) {
        return newDagEntry(contents, ordered, stored, links.stream().map(e -> e.toHash()).collect(Collectors.toList()),
                           null);
    }

    HASH newDagEntry(String contents, List<HashKey> ordered, Map<HashKey, DagEntry> stored, List<HASH> links) {
        return newDagEntry(contents, ordered, stored, links, null);
    }

    HASH newDagEntry(String contents, List<HashKey> ordered, Map<HashKey, DagEntry> stored, List<HASH> links,
                     HASH conflictSet) {
        DagEntry entry = new DagEntry();
        entry.setData(ByteBuffer.wrap(contents.getBytes()));
        entry.setLinks(links);
        HASH key = dag.putDagEntry(entry, serialize(entry), conflictSet, create, false, 0);
        stored.put(new HashKey(key), entry);
        ordered.add(new HashKey(key));
        return key;
    }

}
