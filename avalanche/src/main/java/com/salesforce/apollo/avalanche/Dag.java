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
import static com.salesforce.apollo.protocols.Conversion.hashOf;
import static com.salesforce.apollo.protocols.Conversion.manifestDag;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.h2.jdbc.JdbcSQLTimeoutException;
import org.h2.tools.TriggerAdapter;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.EntryType;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.dagwood.schema.tables.Closure;
import com.salesforce.apollo.dagwood.schema.tables.records.DagRecord;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class Dag {
    /**
     * H2 after delete trigger to gc links, transitive closure and conflict sets
     */
    public static class DagDeleteTrigger extends TriggerAdapter {

        @Override
        public void fire(Connection conn, ResultSet oldRow, ResultSet newRow) throws SQLException {
            byte[] cs = oldRow.getBytes("CONFLICTSET");
            assert cs != null;
            DSLContext create = DSL.using(conn, SQLDialect.H2);
            create.deleteFrom(CLOSURE)
                  .where(CLOSURE.PARENT.eq(oldRow.getBytes("HASH")))
                  .execute();
            Record1<Integer> result = create.select(CONFLICTSET.CARDINALITY)
                                            .from(CONFLICTSET)
                                            .where(CONFLICTSET.NODE.eq(cs))
                                            .fetchOne();
            if (result == null) {
                return; // row has been deleted
            }
            if (result.value1() <= 1) {
                create.deleteFrom(CONFLICTSET).where(CONFLICTSET.NODE.eq(cs)).execute();
            } else {
                create.update(CONFLICTSET)
                      .set(CONFLICTSET.CARDINALITY, CONFLICTSET.CARDINALITY.minus(DSL.inline(1)))
                      .where(CONFLICTSET.NODE.eq(cs))
                      .execute();
            }
        }
    }

    public static class DagInsert {
        public final HASH conflictSet;
        public final DagEntry dagEntry;
        public final Entry entry;
        public final HashKey key;
        public final boolean noOp;
        public final int targetRound;

        public DagInsert(HASH key, DagEntry dagEntry, Entry entry, HASH conflictSet, boolean noOp, int targetRound) {
            this.key = new HashKey(key);
            this.dagEntry = dagEntry;
            this.entry = entry;
            this.conflictSet = conflictSet;
            this.noOp = noOp;
            this.targetRound = targetRound;
        }
    }

    public static class FinalizationData {
        public final Set<HashKey> deleted = new TreeSet<>();
        public final Set<HashKey> finalized = new TreeSet<>();
    }

    public static class KeyAndEntry {
        public final Entry entry;
        public final byte[] key;

        public KeyAndEntry(byte[] key, Entry entry) {
            this.key = key;
            this.entry = entry;
        }

        @Override
        public String toString() {
            return new HashKey(key).toString();
        }
    }

    public static final byte[] GENESIS_CONFLICT_SET = new byte[32];

    private static Logger log = LoggerFactory.getLogger(Dag.class);

    public static void prefer(Connection conn, byte[] entry, int depth, byte[] conflictSet, int confidence,
            byte[] preferred, int preferredConfidence, byte[] last) {
        updatePreference(entry, depth != 0, conflictSet, confidence, preferred, preferredConfidence, last,
                         DSL.using(conn, SQLDialect.H2));
    }

    /**
     * Update the preference in the parent node
     * 
     * @param last
     */
    static void updatePreference(byte[] entry, boolean closure, byte[] conflictSet, int confidence,
            byte[] preferred, int preferredConfidence, byte[] last, DSLContext create) {

        if (closure) {
            create.update(DAG)
                  .set(DAG.CONFIDENCE, DAG.CONFIDENCE.plus(DSL.inline(1)))
                  .where(DAG.HASH.eq(entry))
                  .execute();
            confidence++;
        }

        if (conflictSet == null) {
            return; // Someone deleted or finalized this
        }
        if (Arrays.equals(preferred, entry)) {
            create.update(CONFLICTSET)
                  .set(CONFLICTSET.LAST, entry)
                  .set(CONFLICTSET.COUNTER, CONFLICTSET.COUNTER.plus(1))
                  .where(CONFLICTSET.NODE.eq(conflictSet))
                  .execute();
            return;
        }

        if (confidence > preferredConfidence) {
            create.update(CONFLICTSET)
                  .set(CONFLICTSET.PREFERRED, entry)
                  .where(CONFLICTSET.NODE.eq(conflictSet))
                  .execute();
        }

        // If node is not the last sampled of the conflict set, set it to so and zero the counter
        if (!Arrays.equals(entry, last)) {
            create.update(CONFLICTSET)
                  .set(CONFLICTSET.LAST, entry)
                  .set(CONFLICTSET.COUNTER, DSL.inline(0))
                  .where(CONFLICTSET.NODE.eq(conflictSet))
                  .execute();
        } else {
            // increment the counter
            create.update(CONFLICTSET)
                  .set(CONFLICTSET.COUNTER, CONFLICTSET.COUNTER.plus(DSL.inline(1)))
                  .where(CONFLICTSET.NODE.eq(conflictSet))
                  .execute();
        }
    }

    private final SecureRandom entropy;
    private final AvalancheParameters parameters;

    public Dag(AvalancheParameters parameters, SecureRandom entropy) {
        this.entropy = entropy;
        this.parameters = parameters;
    }

    /**
     * Add the node to the conflict set
     * 
     * @param node
     * @param conflictSet
     * @param create
     */
    public void addToConflictSet(HASH node, HASH conflictSet, DSLContext create) {
        if (create.select(CONFLICTSET.NODE)
                  .from(CONFLICTSET)
                  .where(CONFLICTSET.NODE.eq(conflictSet.bytes()))
                  .fetchOne() == null) {
            create.insertInto(CONFLICTSET)
                  .set(CONFLICTSET.NODE, conflictSet.bytes())
                  .set(CONFLICTSET.PREFERRED, node.bytes())
                  .set(CONFLICTSET.LAST, node.bytes())
                  .set(CONFLICTSET.CARDINALITY, 0)
                  .execute();
        }
        create.update(DAG).set(DAG.CONFLICTSET, conflictSet.bytes()).where(DAG.HASH.eq(node.bytes())).execute();
        create.update(CONFLICTSET)
              .set(CONFLICTSET.CARDINALITY, CONFLICTSET.CARDINALITY.plus(1))
              .where(CONFLICTSET.NODE.eq(conflictSet.bytes()))
              .execute();
    }

    /**
     * Answer the transitive closure of the node. The iteration order of the closure set is the topological sorted
     * ordering, excluding the root node.
     * 
     * @param node
     *            - the root node
     * @param create
     * @return the transitive set of hashes that are parents of the supplied node
     */
    public Stream<HASH> closure(HASH node, DSLContext create) {
        return create.select(CLOSURE.CHILD)
                     .from(CLOSURE)
                     .where(CLOSURE.PARENT.eq(node.bytes()))
                     .and(CLOSURE.DEPTH.notEqual(DSL.inline(0)))
                     .stream()
                     .map(r -> new HASH(r.value1()));
    }

    /**
     * @param key
     * @return null if no entry is found, otherwise the DagEntry matching the key
     */
    public DagEntry getDagEntry(HASH key, DSLContext create) {
        DagRecord entry = create.selectFrom(DAG).where(DAG.HASH.eq(key.bytes())).fetchOne();
        if (entry == null) { return null; }
        return manifestDag(new Entry(EntryType.DAG, ByteBuffer.wrap(entry.getData())));
    }

    /**
     * @param want
     * @param limit
     * @return the list of existing entries from the list, up to the limit provided
     */
    public List<Entry> getEntries(List<HASH> want, int limit, DSLContext create) {
        if (want.isEmpty()) { return Collections.emptyList(); }
        Table<Record> wanted = DSL.table("WANTED");
        Field<byte[]> wantedHash = DSL.field("HASH", byte[].class, wanted);
        create.execute("create cached local temporary table if not exists WANTED(HASH binary(32))");

        BatchBindStep batch = create.batch(create.insertInto(wanted, wantedHash).values((byte[])null));
        want.forEach(e -> batch.bind(e.bytes()));
        try {
            batch.execute();
            List<Entry> retrieved;
            retrieved = create.select(DAG.DATA)
                              .from(DAG)
                              .join(wanted)
                              .on(DAG.HASH.eq(DSL.field("WANTED.HASH", byte[].class, wanted)))
                              .limit(limit)
                              .fetch()
                              .stream()
                              .map(r -> entryFrom((byte[])r.get(0)))
                              .collect(Collectors.toList());
            create.delete(wanted);
            return retrieved;
        } catch (DataAccessException e) {
            log.trace("Unable to select wanted: {}", e.toString());
            return Collections.emptyList();
        }
    }

    public Stream<HASH> getNeglected(DSLContext create) {
        return create.select(DAG.HASH)
                     .from(DAG)
                     .where(DAG.NOOP.isFalse())
                     .and(DAG.FINALIZED.isFalse())
                     .and(DAG.CONFIDENCE.le(DSL.inline(parameters.beta1 / 2)))
                     .orderBy(DSL.rand())
                     .stream()
                     .map(r -> new HASH(r.value1()));
    }

    /**
     * @return the non noOp transactions on the frontier in the DAG that are currently neglegected
     */
    public Stream<HASH> getNeglectedFrontier(DSLContext create) {
        return create.select(DAG.HASH)
                     .from(DAG)
                     .join(CONFLICTSET)
                     .on(CONFLICTSET.NODE.eq(DAG.CONFLICTSET))
                     .where(DAG.FINALIZED.isFalse()
                                         .and(DAG.NOOP.isFalse())
                                         .and(DAG.CONFIDENCE.le(DSL.inline(3))
                                                            .and(CONFLICTSET.CARDINALITY.eq(DSL.inline(1)))))
                     .and(DAG.HASH.in(create.select(DAG.HASH)
                                            .from(DAG)
                                            .leftAntiJoin(LINK)
                                            .on(LINK.HASH.eq(DAG.HASH))
                                            .where(DAG.FINALIZED.isFalse())
                                            .and(DAG.NOOP.isFalse())))
                     .orderBy(DSL.rand())
                     .stream()
                     .map(r -> new HASH(r.value1()));

    }

    /**
     * @param limit
     * @return the list of nodes referred to by links of existing nodes which are not in the db, up to the indicated
     *         limit
     */
    public List<HASH> getWanted(int limit, DSLContext create) {
        return create.selectDistinct(LINK.HASH)
                     .from(LINK)
                     .leftAntiJoin(DAG)
                     .on(LINK.HASH.eq(DAG.HASH))
                     .limit(limit)
                     .stream()
                     .map(r -> new HASH(r.value1()))
                     .filter(e -> e != null)
                     .collect(Collectors.toList());
    }

    public List<HASH> getWantedSlow(int limit, DSLContext create) {
        return create.selectDistinct(LINK.HASH)
                     .from(LINK)
                     .leftAntiJoin(DAG)
                     .on(LINK.HASH.eq(DAG.HASH))
                     .limit(limit)
                     .stream()
                     .map(r -> new HASH(r.value1()))
                     .filter(e -> e != null)
                     .collect(Collectors.toList());
    }

    public Boolean isFinalized(HASH hash, DSLContext context) {
        Record1<Boolean> fetched = context.select(DAG.FINALIZED).from(DAG).where(DAG.HASH.eq(hash.bytes())).fetchOne();
        return fetched != null ? fetched.value1() : false;
    }

    public boolean isStronglyPreferred(HASH key, DSLContext create) {
        return isStronglyPreferred(Collections.singletonList(key), create).get(0);
    }

    /**
     * Query whether a node is strongly prefered by the current state of the DAG. A node is strongly preferred if it is
     * the preferred node of its conflict set and every parent of the node is also the preferred node of its conflict
     * set.
     * <p>
     * Because we optimize by lazily fetching DAG nodes, any given node may not have all its parents in the DAG state.
     * Consequently if there are dangling references of the node, then the node cannot be judged preferred. Finalized
     * nodes are considered to stand in for any parents, and thus cut off any further querying past these finalized
     * parents
     * 
     * @param node
     * @return true if the corresponding node is strongly preferred, false if null or not strongly preferred
     */
    public List<Boolean> isStronglyPreferred(List<HASH> keys, DSLContext create) {
        if (keys.isEmpty()) { return Collections.emptyList(); }
        long now = System.currentTimeMillis();
        Table<Record> queried = DSL.table("queried");

        Field<byte[]> queriedHash = DSL.field("queried.hash", byte[].class, queried);
        create.execute("create cached local temporary table if not exists queried(hash binary(32))");
        BatchBindStep batch = create.batch(create.insertInto(queried, DSL.field("hash", byte[].class))
                                                 .values((byte[])null));
        keys.forEach(e -> batch.bind(e.bytes()));
        batch.execute();

        Field<Integer> closureCount = create.selectCount()
                                            .from(CLOSURE)
                                            .where(CLOSURE.PARENT.eq(DAG.HASH))
                                            .asField();

        com.salesforce.apollo.dagwood.schema.tables.Dag child = DAG.as("child");
        Field<Integer> accepted = create.selectCount()
                                        .from(CLOSURE)
                                        .join(child)
                                        .on(CLOSURE.CHILD.eq(child.field(DAG.HASH)))
                                        .join(CONFLICTSET)
                                        .on(CONFLICTSET.NODE.eq(child.field(DAG.CONFLICTSET)))
                                        .where(CLOSURE.PARENT.eq(DAG.HASH))
                                        .and(child.field(DAG.FINALIZED)
                                                  .isTrue()
                                                  .or(CONFLICTSET.PREFERRED.eq(child.field(DAG.HASH))))
                                        .asField();
        List<Boolean> result = create.select(DSL.when(DAG.HASH.isNull(), false)
                                                .when(DAG.FINALIZED.isTrue(), true)
                                                .when(closureCount.eq(accepted), true)
                                                .otherwise(false))
                                     .from(queried)
                                     .leftOuterJoin(DAG)
                                     .on(DAG.HASH.eq(queriedHash))
                                     .stream()
                                     .map(r -> r.value1())
                                     .collect(Collectors.toList());
        create.delete(queried).execute();
        log.debug("isStrongly preferred: {} in {} ms", keys.size(), System.currentTimeMillis() - now);
        return result;
    }

    public void markQueried(List<byte[]> nodes, DSLContext create) {
        long now = System.currentTimeMillis();
        Table<Record> toMark = DSL.table("TO_MARK");
        Field<byte[]> toMarkHash = DSL.field("HASH", byte[].class, toMark);
        create.execute("create cached local temporary table if not exists TO_MARK(HASH binary(32))");

        BatchBindStep batch = create.batch(create.insertInto(toMark, toMarkHash).values((byte[])null));
        nodes.forEach(e -> batch.bind(e));
        batch.execute();
        create.deleteFrom(UNQUERIED)
              .where(UNQUERIED.HASH.in(create.select(toMarkHash).from(toMark)))
              .execute();
        create.deleteFrom(toMark).execute();
        log.debug("Mark queried {} in {} ms", nodes.size(), System.currentTimeMillis() - now);
    }

    public void prefer(HASH entry, DSLContext create) {
        prefer(Collections.singletonList(entry), create);
    }

    /**
     * The vote is in. Prefer this entry. Update the chit of this entry to TRUE and thus the confidence values of the
     * parent closure set. Update the preferences and counters of the parent's conflict sets.
     * 
     * @param entry
     */
    public void prefer(List<HASH> entries, DSLContext create) {
        long start = System.currentTimeMillis();
        create.execute("create cached local temporary table if not exists TO_PREFER(HASH binary(32))");
        create.execute("create cached local temporary table if not exists PREFERRED(PARENT binary(32), CHILD binary(32), DEPTH INT, CONFLICT_SET binary(32), CONFLICT_SET_COUNTER INT, CONFIDENCE INT, CS_PREFERRED binary(32), PREFERRED_CONFIDENCE INT, LAST binary(32))");

        Table<Record> toPrefer = DSL.table("TO_PREFER");
        Field<byte[]> toPreferHash = DSL.field("HASH", byte[].class, toPrefer);
        BatchBindStep batch = create.batch(create.insertInto(toPrefer, toPreferHash).values((byte[])null));
        entries.forEach(e -> batch.bind(e.bytes()));
        batch.execute();

        com.salesforce.apollo.dagwood.schema.tables.Dag preferred = DAG.as("preferred");

        // create.select(DAG.HASH)
        // .from(DAG)
        // .join(CLOSURE)
        // .on(DAG.HASH.eq(CLOSURE.CHILD))
        // .join(toPrefer)
        // .on(DSL.field("TO_PREFER.HASH", byte[].class, toPrefer).eq(CLOSURE.PARENT))
        // .orderBy(DAG.HASH)
        // .forUpdate();

        Table<Record> p = DSL.table("PREFERRED");
        Field<byte[]> pParent = DSL.field("PARENT", byte[].class, p);
        Field<byte[]> pChild = DSL.field("CHILD", byte[].class, p);
        Field<Integer> pDepth = DSL.field("DEPTH", Integer.class, p);
        Field<byte[]> pCs = DSL.field("CONFLICT_SET", byte[].class, p);
        Field<Integer> pConfidence = DSL.field("CONFIDENCE", Integer.class, p);
        Field<Integer> pCsCounter = DSL.field("CONFLICT_SET_COUNTER", Integer.class, p);
        Field<byte[]> pPreferred = DSL.field("CS_PREFERRED", byte[].class, p);
        Field<Integer> pPreferredConfidence = DSL.field("PREFERRED_CONFIDENCE", Integer.class, p);
        Field<byte[]> pLast = DSL.field("LAST", byte[].class, p);

        int updated = create.mergeInto(p, pParent, pChild, pDepth, pCs, pCsCounter, pConfidence, pPreferred,
                                       pPreferredConfidence, pLast)
                            .key(pParent, pChild)
                            .select(create.select(CLOSURE.PARENT, CLOSURE.CHILD, CLOSURE.DEPTH, CONFLICTSET.NODE,
                                                  CONFLICTSET.COUNTER,
                                                  DAG.CONFIDENCE,
                                                  preferred.field(DAG.HASH),
                                                  preferred.field(DAG.CONFIDENCE), CONFLICTSET.LAST)
                                          .from(CLOSURE)
                                          .join(DAG)
                                          .on(DAG.HASH.eq(CLOSURE.CHILD))
                                          .join(CONFLICTSET)
                                          .on(CONFLICTSET.NODE.eq(DAG.CONFLICTSET))
                                          .join(preferred)
                                          .on(preferred.field(DAG.HASH).eq(CONFLICTSET.PREFERRED))
                                          .join(toPrefer)
                                          .on(DSL.field("TO_PREFER.HASH", byte[].class, toPrefer).eq(CLOSURE.PARENT)))
                            .execute();

        // System.out.println("before \n" + create.selectFrom(p).fetch());

        create.update(DAG).set(DAG.CHIT, 1).where(DAG.HASH.in(create.select(toPreferHash).from(toPrefer))).execute();
        create.update(DAG)
              .set(DAG.CONFIDENCE, DAG.CONFIDENCE.plus(1))
              .where(DAG.HASH.in(create.selectDistinct(pChild).from(p).where(pDepth.gt(0)).orderBy(pChild)))
              .execute();
        create.mergeInto(CONFLICTSET, CONFLICTSET.NODE, CONFLICTSET.LAST, CONFLICTSET.PREFERRED, CONFLICTSET.COUNTER)
              .key(CONFLICTSET.NODE)
              .select(create.select(pCs, pChild,
                                    DSL.when(pConfidence.plus(1).gt(pPreferredConfidence), pChild)
                                       .otherwise(pPreferred),
                                    DSL.when(pChild.eq(pLast), pCsCounter.plus(1)).otherwise(0))
                            .from(p))
              .execute();

        // System.out.println("after \n" + create
        // .select(CLOSURE.CHILD, CLOSURE.DEPTH, CONFLICTSET.NODE,
        // CONFLICTSET.COUNTER, DAG.CONFIDENCE,
        // preferred.field(DAG.HASH),
        // preferred.field(DAG.CONFIDENCE), CONFLICTSET.LAST)
        // .from(CLOSURE)
        // .join(DAG)
        // .on(DAG.HASH.eq(CLOSURE.CHILD))
        // .join(CONFLICTSET)
        // .on(CONFLICTSET.NODE.eq(DAG.CONFLICTSET))
        // .join(preferred)
        // .on(preferred.field(DAG.HASH).eq(CONFLICTSET.PREFERRED))
        // .join(toPrefer)
        // .on(DSL.field("TO_PREFER.HASH", byte[].class, toPrefer)
        // .eq(CLOSURE.PARENT))
        // .fetch());
        if (updated != 0) {
            log.trace("Preferred {}:{} in {} ms", updated, entries.size(),
                      System.currentTimeMillis() - start);
        }
        create.deleteFrom(p).execute();
        create.deleteFrom(toPrefer).execute();
    }

    public void put(List<DagInsert> inserts, DSLContext context) {
        Query dagInsert = context.insertInto(DAG, DAG.HASH, DAG.DATA, DAG.NOOP, DAG.CONFLICTSET)
                                 .values((byte[])null, (byte[])null, (Boolean)null, (byte[])null)
                                 .keepStatement(true);
        Query insertLinks = context.insertInto(LINK, LINK.NODE, LINK.HASH)
                                   .values((byte[])null, (byte[])null)
                                   .keepStatement(true);
        BatchBindStep batch = context.batch(insertLinks);
        Query insertUnqueried = context.insertInto(UNQUERIED, UNQUERIED.HASH, UNQUERIED.TARGETROUND)
                                       .values((byte[])null, 0)
                                       .keepStatement(true);

        Query insertClosure0 = context.insertInto(CLOSURE, CLOSURE.PARENT, CLOSURE.CHILD, CLOSURE.DEPTH)
                                      .values((byte[])null, (byte[])null, 0)
                                      .keepStatement(true);
        Query updateConfidence = context.update(DAG)
                                        .set(DAG.CONFIDENCE,
                                             context.select(DSL.cast(DSL.sum(DAG.CHIT), Integer.class))
                                                    .from(DAG)
                                                    .join(CLOSURE)
                                                    .on(DAG.HASH.eq(CLOSURE.PARENT))
                                                    .where(CLOSURE.CHILD.eq((byte[])null)))
                                        .where(DAG.HASH.eq((byte[])null))
                                        .keepStatement(true);
        Query insertConflictSet = context.insertInto(CONFLICTSET, CONFLICTSET.NODE, CONFLICTSET.PREFERRED,
                                                     CONFLICTSET.LAST, CONFLICTSET.CARDINALITY)
                                         .values(DSL.value((byte[])null), DSL.value((byte[])null),
                                                 DSL.value((byte[])null), DSL.inline(1))
                                         .keepStatement(true);

        try {
            put(inserts, context, dagInsert, batch, insertUnqueried, insertClosure0, updateConfidence,
                insertConflictSet);
        } finally {
            dagInsert.close();
            insertLinks.close();
            insertUnqueried.close();
            insertClosure0.close();
            updateConfidence.close();
            insertConflictSet.close();
        }
    }

    public HASH putDagEntry(DagEntry dagEntry, Entry entry, HASH conflictSet, DSLContext create, boolean noOp,
            int targetRound) {
        HASH hash = new HASH(hashOf(entry));
        put(Collections.singletonList(new DagInsert(hash, dagEntry, entry, conflictSet, noOp, targetRound)), create);
        return hash;
    }

    public List<HASH> query(int limit, DSLContext create, int round) {
        Result<Record3<byte[], Integer, byte[]>> unqueried = create.select(UNQUERIED.HASH, UNQUERIED.TARGETROUND,
                                                                           DAG.DATA)
                                                                   .from(UNQUERIED)
                                                                   .join(DAG)
                                                                   .on(DAG.HASH.eq(UNQUERIED.HASH))
                                                                   .where(UNQUERIED.TARGETROUND.le(round))
                                                                   .orderBy(UNQUERIED.TARGETROUND)
                                                                   .limit(limit)
                                                                   .fetch();
        return unqueried.stream()
                        .map(r -> new HASH(r.value1()))
                        .collect(Collectors.toList());
    }

    /**
     * Select a random sample of parents for the transaction. The selection process starts at the DAG frontier and
     * proceeds upwards towards the genesis root
     * 
     * @param parentSize
     *            - the number of parents required
     * @return a list of parent transactions, with up to sample elements
     */
    public List<HASH> selectParents(int parentSize, DSLContext create) {
        Set<HashKey> parents = new TreeSet<>();

        // Start with the frontier of the DAG with confidence > 0. Randomly sample the transactions on the frontier.
        // Require at least 1 parent from the frontier, if available.
        int frontierSample = Math.max(1, entropy.nextInt(Math.max(1, parentSize - 1)));

        Result<Record1<byte[]>> sample = frontier(create);
        if (sample.size() <= frontierSample) {
            parents.addAll(sample.stream().map(r -> new HashKey(r.value1())).collect(Collectors.toList()));
        } else {
            for (int i = 0; i <= frontierSample; i++) {
                parents.add(new HashKey(sample.get(i).value1()));
                if (parents.size() == frontierSample) {
                    break;
                }
            }
        }

        if (parents.size() == parentSize) { return parents.stream().map(e -> e.toHash()).collect(Collectors.toList()); }

        // Next select non finalized nodes that have a low confidence, but are the only member of their conflict set.
        create.select(DAG.HASH)
              .from(DAG)
              .join(CONFLICTSET)
              .on(CONFLICTSET.NODE.eq(DAG.CONFLICTSET))
              .where(DAG.CONFIDENCE.gt(DSL.inline(0)).and(DAG.CONFIDENCE.le(3)))
              .and(DAG.FINALIZED.isFalse())
              .and(DAG.NOOP.isFalse())
              .and(CONFLICTSET.CARDINALITY.eq(1))
              .orderBy(DSL.rand())
              .limit(parents.size() - parentSize)
              .stream()
              .map(r -> new HashKey(r.value1()))
              .filter(e -> parents.size() < parentSize)
              .forEach(h -> parents.add(h));

        if (parents.size() == parentSize) { return parents.stream().map(e -> e.toHash()).collect(Collectors.toList()); }

        // Next select any non finalized nodes that are the only member of their conflict set
        create.select(DAG.HASH)
              .from(DAG)
              .join(CONFLICTSET)
              .on(CONFLICTSET.NODE.eq(DAG.CONFLICTSET))
              .where(DAG.FINALIZED.isFalse())
              .and(DAG.NOOP.isFalse())
              .and(CONFLICTSET.CARDINALITY.eq(1))
              .orderBy(DSL.rand())
              .limit(parents.size() - parentSize)
              .stream()
              .map(r -> new HashKey(r.value1()))
              .filter(e -> parents.size() < parentSize)
              .forEach(h -> parents.add(h));

        if (parents.size() == parentSize) { return parents.stream().map(e -> e.toHash()).collect(Collectors.toList()); }

        // Next, select any nodes that are not finalized
        create.select(DAG.HASH)
              .from(DAG)
              .where(DAG.FINALIZED.isFalse())
              .and(DAG.NOOP.isFalse())
              .orderBy(DSL.rand())
              .limit(parents.size() - parentSize)
              .stream()
              .map(r -> new HashKey(r.value1()))
              .filter(e -> parents.size() < parentSize)
              .forEach(h -> parents.add(h));

        if (parents.size() == parentSize) { return parents.stream().map(e -> e.toHash()).collect(Collectors.toList()); }

        // If still none, select any finalized node
        create.select(DAG.HASH)
              .from(DAG)
              .where(DAG.CONFIDENCE.gt(DSL.inline(0)))
              .or(DAG.FINALIZED.isTrue())
              .and(DAG.NOOP.isFalse())
              .orderBy(DSL.rand())
              .limit(parents.size() - parentSize)
              .stream()
              .map(r -> new HashKey(r.value1()))
              .filter(e -> parents.size() < parentSize)
              .forEach(h -> parents.add(h));
        assert parents.size() > 0;

        return parents.stream().map(e -> e.toHash()).collect(Collectors.toList());
    }

    Entry entryFrom(byte[] bytes) {
        return new Entry(EntryType.DAG, ByteBuffer.wrap(bytes));
    }

    // members from close to the frontier of the DAG
    Result<Record1<byte[]>> frontier(DSLContext create) {
        return create.select(LINK.HASH)
                     .from(LINK)
                     .join(DAG)
                     .on(LINK.HASH.eq(DAG.HASH))
                     .join(CONFLICTSET)
                     .on(CONFLICTSET.NODE.eq(DAG.CONFLICTSET))
                     .where(DAG.FINALIZED.isFalse()
                                         .and(DAG.NOOP.isFalse())
                                         .and(DAG.CONFIDENCE.greaterThan(DSL.inline(0))
                                                            .and(CONFLICTSET.CARDINALITY.eq(1))))
                     .and(LINK.NODE.in(create.select(DAG.HASH)
                                             .from(DAG)
                                             .leftAntiJoin(LINK)
                                             .on(LINK.HASH.eq(DAG.HASH))
                                             .where(DAG.FINALIZED.isFalse())
                                             .and(DAG.NOOP.isFalse())))
                     .orderBy(DSL.rand())
                     .limit(100)
                     .fetch();
    }

    Stream<HASH> frontierSample(DSLContext create) {
        return create.select(LINK.HASH)
                     .from(LINK)
                     .join(DAG)
                     .on(LINK.HASH.eq(DAG.HASH))
                     .join(CONFLICTSET)
                     .on(CONFLICTSET.NODE.eq(DAG.CONFLICTSET))
                     .where(DAG.FINALIZED.isFalse()
                                         .and(DAG.NOOP.isFalse())
                                         .and(DAG.CONFIDENCE.ge(DSL.inline(0))
                                                            .and(DAG.CONFIDENCE.le(DSL.inline(parameters.beta1 / 1)))
                                                            .and(CONFLICTSET.CARDINALITY.eq(DSL.inline(1)))))
                     .and(LINK.NODE.in(create.select(DAG.HASH)
                                             .from(DAG)
                                             .leftAntiJoin(LINK)
                                             .on(LINK.HASH.eq(DAG.HASH))
                                             .where(DAG.FINALIZED.isFalse())
                                             .and(DAG.NOOP.isFalse())))
                     .orderBy(DSL.rand())
                     .stream()
                     .map(e -> new HASH(e.value1()));
    }

    // members from within the middle of the DAG
    Result<Record2<byte[], Integer>> middling(DSLContext create) {
        return create.selectDistinct(CLOSURE.CHILD, CLOSURE.DEPTH)
                     .from(CLOSURE)
                     .join(DAG)
                     .on(CLOSURE.CHILD.eq(DAG.HASH).and(CLOSURE.DEPTH.gt(DSL.inline(1))))
                     .join(CONFLICTSET)
                     .on(CONFLICTSET.NODE.eq(DAG.CONFLICTSET))
                     .where(CLOSURE.PARENT.in(create.select(DAG.HASH)
                                                    .from(DAG)
                                                    .leftAntiJoin(CLOSURE)
                                                    .on(CLOSURE.DEPTH.gt(1).and(CLOSURE.CHILD.eq(DAG.HASH)))))
                     .and(DAG.FINALIZED.isFalse()
                                       .and(DAG.NOOP.isFalse())
                                       .and(DAG.CONFIDENCE.greaterThan(DSL.inline(0))))
                     .orderBy(CLOSURE.DEPTH)
                     .limit(100)
                     .fetch();
    }

    void put(List<DagInsert> inserts, DSLContext context, Query dagInsert, BatchBindStep linkBatch,
            Query insertUnqueried, Query insertClosure0, Query updateConfidence, Query insertConflictSet) {
        Closure p = CLOSURE.as("p");
        Closure c = CLOSURE.as("c");

        inserts.forEach(insert -> {
            HASH conflictSet = insert.conflictSet;
            conflictSet = insert.dagEntry.getLinks() == null ? new HASH(GENESIS_CONFLICT_SET)
                                                             : conflictSet == null ? insert.key.toHash() : conflictSet;
            if (insert.dagEntry.getLinks() == null) {
                System.out.println("Genesis entry: " + insert.key);
            }
            try {
                if (!context.fetchExists(DAG, DAG.HASH.eq(insert.key.bytes()))) {
                    dagInsert.bind(1, insert.key.bytes())
                             .bind(2, insert.entry.getData().array())
                             .bind(3, insert.noOp)
                             .bind(4, conflictSet.bytes())
                             .execute();
                    insertUnqueried.bind(1, insert.key.bytes()).bind(2, insert.targetRound).execute();
                    insertClosure0.bind(1, insert.key.bytes())
                                  .bind(2, insert.key.bytes())
                                  .execute();
                    if (insert.dagEntry.getLinks() != null) {
                        if (insert.dagEntry.getLinks().isEmpty()) {
                            System.out.println("Empty link entry: " + insert.key);
                        }
                        insert.dagEntry.getLinks()
                                       .forEach(link -> linkBatch.bind(insert.key.bytes(), link.bytes()));
                        linkBatch.execute();
                        context.mergeInto(CLOSURE, CLOSURE.PARENT, CLOSURE.CHILD, CLOSURE.DEPTH)
                               .key(CLOSURE.PARENT, CLOSURE.CHILD)
                               .select(context.select(p.PARENT, c.CHILD, DSL.inline(1))
                                              .from(p, c)
                                              .join(LINK)
                                              .on(LINK.NODE.eq(insert.key.bytes()))
                                              .where(p.CHILD.eq(insert.key.bytes()))
                                              .and(c.PARENT.eq(LINK.HASH)))
                               .execute();
                    }
                    if (context.fetchExists(CONFLICTSET, CONFLICTSET.NODE.eq(conflictSet.bytes()))) {
                        context.update(CONFLICTSET)
                               .set(CONFLICTSET.CARDINALITY, CONFLICTSET.CARDINALITY.plus(1))
                               .where(CONFLICTSET.NODE.eq(conflictSet.bytes()))
                               .execute();
                    } else {
                        insertConflictSet.bind(1, conflictSet.bytes())
                                         .bind(2, insert.key.bytes())
                                         .bind(3, insert.key.bytes())
                                         .execute();
                    }

                    if (!insert.noOp) {
                        updateConfidence.bind(1, insert.key.bytes()).bind(2, insert.key.bytes()).execute();
                    }
                }
            } catch (DataAccessException e) {
                if (e.getCause() instanceof JdbcSQLTimeoutException) {
                    log.error("Concurrency failure", e.getCause());
                }
            }
            log.trace("inserted: {}", insert.key);
        });
    }

    /**
     * Finalize the transactions that are eligible for commit in the closure of the supplied keys (note, the closure
     * includes the node itself ;) ). There are two ways a transaction can finalize: safe early commit and late
     * finalization, where the confidence from the node's progeny is high enough to commit whp.
     * <p>
     * Safe early commit is when the node is the preferred and only member of its conflict set, and the confidence of
     * that node is greater than the parameter value beta1.
     * <p>
     * Finalization - "late" finalization" - is determined when the counter of a node's partition is greater than the
     * parameter value beta2.
     * <p>
     * Note that the "confidence" value of a node is strictly determined as the sum of all chits (which can only take 0
     * or 1 value and are immutable once determined) of the progeny (children) of a node. That is, newer transactions
     * that use this node as a parent in its transitive closure will "vote" for the transitive closure of parents.
     */
    FinalizationData tryFinalize(List<byte[]> keys, DSLContext context) {
        assert !keys.isEmpty();
        long start = System.currentTimeMillis();

        Table<Record> toQuery = DSL.table("TO_QUERY");
        Field<byte[]> toQueryHash = DSL.field("HASH", byte[].class, toQuery);
        context.execute("create cached local temporary table if not exists TO_QUERY(HASH binary(32) unique)");
        context.execute("CREATE INDEX if not exists unique_query ON TO_QUERY(HASH);");

        Table<Record> allFinalized = DSL.table("ALL_FINALIZED");
        Field<byte[]> allFinalizedHash = DSL.field("HASH", byte[].class, allFinalized);
        context.execute("create cached local temporary table if not exists ALL_FINALIZED(HASH binary(32) unique)");
        context.execute("CREATE INDEX if not exists unique_finalized ON ALL_FINALIZED(HASH);");

        Field<Integer> closureCount = context.selectCount()
                                             .from(CLOSURE)
                                             .where(CLOSURE.PARENT.eq(DAG.HASH))
                                             .asField();

        com.salesforce.apollo.dagwood.schema.tables.Dag child = DAG.as("child");
        Field<Integer> accepted = context.selectCount()
                                         .from(CLOSURE)
                                         .join(child)
                                         .on(CLOSURE.CHILD.eq(child.field(DAG.HASH)))
                                         .join(CONFLICTSET)
                                         .on(CONFLICTSET.NODE.eq(child.field(DAG.CONFLICTSET)))
                                         .where(CLOSURE.PARENT.eq(DAG.HASH))
                                         .and(child.field(DAG.FINALIZED)
                                                   .isTrue()
                                                   .or(CONFLICTSET.CARDINALITY.eq(1)
                                                                              .and(child.field(DAG.CONFIDENCE)
                                                                                        .gt(parameters.beta1))
                                                                              .or(CONFLICTSET.COUNTER.gt(parameters.beta2)
                                                                                                     .and(CONFLICTSET.PREFERRED.eq(child.field(DAG.HASH))))))
                                         .asField();
        SelectConditionStep<Record1<byte[]>> selectFinalized;
        selectFinalized = context.select(DAG.HASH)
                                 .from(DAG)
                                 .where(DAG.HASH.in(context.select(toQueryHash).from(toQuery)))
                                 .and(DAG.FINALIZED.isFalse())
                                 .and(closureCount.eq(accepted));

        BatchBindStep batch = context.batch(context.insertInto(toQuery, toQueryHash).values((byte[])null));
        keys.forEach(e -> batch.bind(e));
        batch.execute();

        com.salesforce.apollo.dagwood.schema.tables.Dag childe = DAG.as("childe");
        context.mergeInto(toQuery, toQueryHash)
               .key(toQueryHash)
               .select(context.selectDistinct(CLOSURE.CHILD)
                              .from(CLOSURE)
                              .join(toQuery)
                              .on(CLOSURE.PARENT.eq(DSL.field("TO_QUERY.HASH", byte[].class)))
                              .and(CLOSURE.DEPTH.gt(0))
                              .join(childe)
                              .on(CLOSURE.CHILD.eq(childe.field(DAG.HASH)))
                              .and(childe.field(DAG.FINALIZED).isFalse()))
               .execute();

        context.mergeInto(allFinalized, allFinalizedHash)
               .key(allFinalizedHash)
               .select(selectFinalized)
               .execute();

        context.mergeInto(allFinalized, allFinalizedHash)
               .key(allFinalizedHash)
               .select(context.select(CLOSURE.CHILD)
                              .from(CLOSURE)
                              .join(allFinalized)
                              .on(CLOSURE.PARENT.eq(allFinalizedHash)))
               .execute();

        // context.select(DAG.HASH)
        // .from(DAG)
        // .join(allFinalized)
        // .on(DSL.field("ALL_FINALIZED.HASH", byte[].class, allFinalized).eq(DAG.HASH))
        // .forUpdate()
        // .execute();
        int updated = context.update(DAG)
                             .set(DAG.FINALIZED, DSL.inline(true))
                             .where(DAG.HASH.in(context.select(allFinalizedHash).from(allFinalized)))
                             .and(DAG.FINALIZED.isFalse())
                             .returning(DAG.HASH)
                             .execute();

        Closure c = CLOSURE.as("c");
        // int closure = 0;
        int closure = context.deleteFrom(CLOSURE)
                             .where(CLOSURE.CHILD.in(context.selectDistinct(c.field(CLOSURE.CHILD))
                                                            .from(c)
                                                            .join(allFinalized)
                                                            .on(c.field(CLOSURE.PARENT).eq(allFinalizedHash))))
                             .execute();
        FinalizationData data = new FinalizationData();
        context.select(allFinalizedHash)
               .from(allFinalized)
               .stream()
               .map(r -> new HashKey(r.value1()))
               .forEach(k -> data.finalized.add(k));
        if (updated != 0) {
            log.trace("Finalized {}:{}:{} out of {} in {} ms", updated, data.finalized.size(), closure, keys.size(),
                      System.currentTimeMillis() - start);
        }

        context.delete(toQuery).execute();
        context.delete(allFinalized).execute();
        return data;
    }

    /**
     * Maintain the transitive closure of the DAG
     * 
     * @param key
     * @param noOp
     */
    void updateClosure(HashKey key, DSLContext create, boolean noOp) {
        Closure p = CLOSURE.as("p");
        Closure c = CLOSURE.as("c");

        create.mergeInto(CLOSURE, CLOSURE.PARENT, CLOSURE.CHILD, CLOSURE.DEPTH)
              .key(CLOSURE.PARENT, CLOSURE.CHILD)
              .select(create.select(p.PARENT, c.CHILD, DSL.inline(1))
                            .from(p, c)
                            .join(LINK)
                            .on(LINK.NODE.eq(key.bytes()))
                            .where(p.CHILD.eq(key.bytes()))
                            .and(c.PARENT.eq(LINK.HASH)))
              .execute();
    }
}
