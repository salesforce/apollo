/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.dagwood.schema.Tables.PROCESSORS;
import static com.salesforce.apollo.protocols.Conversion.hashOf;
import static com.salesforce.apollo.protocols.Conversion.manifestDag;
import static com.salesforce.apollo.protocols.Conversion.serialize;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.avro.AvroRemoteException;
import org.h2.jdbc.JdbcSQLTransactionRollbackException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avalanche.Dag.DagInsert;
import com.salesforce.apollo.avalanche.Dag.FinalizationData;
import com.salesforce.apollo.avalanche.communications.AvalancheClientCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.EntryType;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.QueryResult;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.RandomMemberGenerator;
import com.salesforce.apollo.fireflies.Ring;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.MessageChannelHandler;
import com.salesforce.apollo.protocols.HashKey;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * Implementation of the Avalanche consensus protocol.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Avalanche {

    public class Listener implements MessageChannelHandler {
        private final AtomicBoolean pending = new AtomicBoolean();

        @Override
        public void message(List<Msg> messages) {
            if (!running.get()) { return; }
            int r = round.get() + rounds2Flood;
            ForkJoinPool.commonPool().execute(() -> {
                messages.forEach(message -> {
                    Entry entry = new Entry(EntryType.DAG, ByteBuffer.wrap(message.content));
                    DagEntry dagEntry = manifestDag(entry);
                    insertions.add(new HASH(hashOf(entry)), dagEntry, entry, null, dagEntry.getDescription() == null,
                                   r);
                });
            });
        }

        public void round() {
            int r = round.incrementAndGet();
            if (!running.get()) { return; }
            if (!pending.compareAndSet(false, true)) {
                log.info("busy {}", phase.get());
                return;
            }
            roundExecutor.execute(() -> {
                try {
                    if (r % parameters.epsilon == 0) {
                        phase.set("query transactions");
                        Avalanche.this.round();
                    }
                    if (r % parameters.delta == 0) {
                        phase.set("generate NoOp transactions");
                        generateNoOpTxns();
                    }
                } finally {
                    pending.set(false);
                    phase.set("Round complete");
                }
            });
        }
    }

    public static class PendingTransaction {
        public final CompletableFuture<HashKey> pending;
        public final ScheduledFuture<?> timer;

        public PendingTransaction(CompletableFuture<HashKey> pending, ScheduledFuture<?> timer) {
            this.pending = pending;
            this.timer = timer;
        }

        public void complete(HashKey key) {
            log.trace("Finalizing transaction: {}", key);
            timer.cancel(true);
            if (pending != null) {
                pending.complete(key);
            }
        }

    }

    public class Service {

        public QueryResult onQuery(List<HASH> transactions, List<HASH> want) {
            if (!running.get()) {
                ArrayList<Boolean> results = new ArrayList<>();
                Collections.fill(results, false);
                return new QueryResult(results, new ArrayList<>());
            }
            long now = System.currentTimeMillis();
            QueryResult result = new QueryResult(
                                                 queryPool.transactionResult(config -> dag.isStronglyPreferred(transactions,
                                                                                                               DSL.using(config))),
                                                 queryPool.transactionResult(config -> dag.getEntries(want,
                                                                                                      parameters.limit,
                                                                                                      DSL.using(config))));
            assert result.getResult().size() == transactions.size() : "on query results " + result.getResult().size()
                    + " != " + transactions.size();
            log.debug("onquery {} txn in {} ms", transactions.size(), System.currentTimeMillis() - now);
            return result;
        }

        public List<Entry> requestDAG(List<HASH> want) {
            if (!running.get()) { return new ArrayList<>(); }
            return dag.getEntries(want, parameters.limit, queryPool);
        }
    }

    public static final int AVALANCHE_TRANSACTION_CHANNEL = 2;

    private static final String DAG_SCHEMA_YML = "dag-schema.yml";
    private final static Logger log = LoggerFactory.getLogger(Avalanche.class);
    private static final String PASSWORD = "";
    private static final String USER_NAME = "Apollo";

    public static void loadSchema(String dbConnect) {
        Connection connection;
        try {
            connection = DriverManager.getConnection(dbConnect, "apollo", PASSWORD);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot get DB Connection: " + dbConnect, e);
        }
        Database database;
        try {
            database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
        } catch (DatabaseException e) {
            throw new IllegalStateException("Unable to get DB factory for: " + dbConnect, e);
        }
        database.setLiquibaseSchemaName("public");
        Liquibase liquibase = new Liquibase(DAG_SCHEMA_YML,
                                            new ClassLoaderResourceAccessor(Avalanche.class.getClassLoader()),
                                            database);
        try {
            liquibase.update((Contexts)null);
        } catch (LiquibaseException e) {
            throw new IllegalStateException("Cannot load schema", e);
        }

        liquibase = new Liquibase("functions.yml", new ClassLoaderResourceAccessor(Avalanche.class.getClassLoader()),
                                  database);
        try {
            liquibase.update((Contexts)null);
        } catch (LiquibaseException e) {
            throw new IllegalStateException("Cannot load functions", e);
        }

        try {
            connection.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create trigger", e);
        }
    }

    private final AvalancheCommunications comm;
    private final Dag dag;
    private final AtomicInteger finalized = new AtomicInteger();
    private final BlockingDeque<HASH> finalizing = new LinkedBlockingDeque<>();
    private final InsertQ insertions = new InsertQ();
    private final Listener listener = new Listener();
    private final ExecutorService mutator;
    private final AvalancheParameters parameters;
    private final ConcurrentMap<HashKey, PendingTransaction> pendingTransactions = new ConcurrentSkipListMap<>();
    private final AtomicReference<String> phase = new AtomicReference<>();
    private final BlockingDeque<HASH> preferings = new LinkedBlockingDeque<>();
    private final Map<HashKey, EntryProcessor> processors = new ConcurrentSkipListMap<>();
    private final DSLContext queryPool;
    private final int required;
    private final AtomicInteger round = new AtomicInteger();
    private final ExecutorService roundExecutor;
    private final DSLContext roundPool;
    private final int rounds2Flood;
    private final AtomicBoolean running = new AtomicBoolean();
    private final RandomMemberGenerator sampler;
    private final Service service = new Service();
    private final DSLContext submitPool;
    private final View view;

    public Avalanche(View view, AvalancheCommunications communications, AvalancheParameters p) {
        this(view, communications, p, null);
    }

    public Avalanche(View view, AvalancheCommunications communications, AvalancheParameters p, ClassLoader resolver) {
        parameters = p;
        this.view = view;
        this.comm = communications;
        comm.initialize(this);
        final HikariConfig queryConfig = new HikariConfig();
        queryConfig.setMinimumIdle(3_000);
        queryConfig.setMaximumPoolSize(parameters.maxQueries);
        queryConfig.setUsername(USER_NAME);
        queryConfig.setPassword(PASSWORD);
        queryConfig.setJdbcUrl(parameters.dbConnect);
        queryConfig.setAutoCommit(false);
        queryPool = DSL.using(new HikariDataSource(queryConfig), SQLDialect.H2);

        String rawConnect = parameters.dbConnect + ";USER=" + Avalanche.USER_NAME + ";PASSWORD=" + Avalanche.PASSWORD;

        roundPool = DSL.using(rawConnect);
        submitPool = DSL.using(rawConnect);

        loadSchema(parameters.dbConnect);
        this.dag = new Dag(parameters, view.getNode().getParameters().entropy);

        view.register(AVALANCHE_TRANSACTION_CHANNEL, listener);
        view.registerRoundListener(() -> listener.round());

        sampler = new RandomMemberGenerator(view);
        required = (int)(parameters.k * parameters.alpha);
        ClassLoader loader = resolver;
        if (resolver == null) {
            loader = Thread.currentThread().getContextClassLoader();
            if (loader == null) {
                loader = getClass().getClassLoader();
            }
        }

        initializeProcessors(loader);
        rounds2Flood = view.getNode().getParameters().toleranceLevel * view.getDiameter() + 1;

        AtomicInteger rT = new AtomicInteger();
        roundExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "Round XO[" + view.getNode().getId() + ":" + rT.incrementAndGet() + "]");
            t.setDaemon(true);
            return t;
        });
        AtomicInteger mT = new AtomicInteger();
        mutator = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "DAG Mutator[" + view.getNode().getId() + ":" + mT.incrementAndGet() + "]");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Create the genesis block for this view
     * 
     * @param data
     *            - the genesis transaction content
     * @param timeout
     *            -how long to wait for finalization of the transaction
     * @return a CompleteableFuture indicating whether the transaction is finalized or not, or whether an exception
     *         occurred that prevented processing. The returned HashKey is the hash key of the the finalized genesis
     *         transaction in the DAG
     */
    public CompletableFuture<HashKey> createGenesis(byte[] data, Duration timeout, ScheduledExecutorService scheduler) {
        if (!running.get()) { throw new IllegalStateException("Service is not running"); }
        CompletableFuture<HashKey> futureSailor = new CompletableFuture<>();
        DagEntry dagEntry = new DagEntry();
        dagEntry.setDescription(WellKnownDescriptions.GENESIS.toHash());
        dagEntry.setData(ByteBuffer.wrap("Genesis".getBytes()));
        // genesis has no parents
        Entry entry = serialize(dagEntry);
        byte[] hash = hashOf(entry);
        insertions.add(new HASH(hash), dagEntry, entry, WellKnownDescriptions.GENESIS.toHash(), false,
                       round.get() + rounds2Flood);
        flood(entry);
        HashKey key = new HashKey(hash);
        pendingTransactions.put(key, new PendingTransaction(futureSailor,
                                                            scheduler.schedule(() -> timeout(key), timeout.toMillis(),
                                                                               TimeUnit.MILLISECONDS)));
        return futureSailor;
    }

    public Dag getDag() {
        return dag;
    }

    public DagDao getDagDao() {
        return new DagDao(dag, submitPool);
    }

    public DSLContext getDslContext() {
        return submitPool;
    }

    public int getFinalized() {
        return finalized.get();
    }

    public Node getNode() {
        return view.getNode();
    }

    public int getRoundCounter() {
        return round.get();
    }

    public Service getService() {
        return service;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) { return; }
        mutator.execute(() -> insertLoop());
        mutator.execute(() -> finalizerLoop());
        comm.start();
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) { return; }
        pendingTransactions.values().forEach(pending -> {
            pending.pending.complete(null);
            pending.timer.cancel(true);
        });
        comm.close();
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param data
     *            - the transaction content
     * @param timeout
     *            -how long to wait for finalization of the transaction
     * @param future
     *            - optional future to be notified of finalization
     * @return the HASH of the transaction, null if invalid
     */
    public HASH submitTransaction(HASH description, byte[] data, Duration timeout, CompletableFuture<HashKey> future,
            ScheduledExecutorService scheduler) {
        if (!running.get()) { throw new IllegalStateException("Service is not running"); }
        DagEntry dagEntry = new DagEntry();
        dagEntry.setDescription(description);
        dagEntry.setData(ByteBuffer.wrap(data));
        List<HASH> parents;
        try {
            parents = dag.selectParents(parameters.parentCount, submitPool).stream().collect(Collectors.toList());
        } catch (Throwable e) {
            if (future != null) {
                future.completeExceptionally(e);
            }
            return null;
        }
        if (parents.isEmpty()) {
            if (future != null) {
                future.completeExceptionally(new IllegalStateException("No parents available for transaction"));
            }
            return null;
        }
        dagEntry.setLinks(parents);
        Entry entry = serialize(dagEntry);
        byte[] hash = hashOf(entry);
        insertions.add(new HASH(hash), dagEntry, entry, null, false, round.get() + rounds2Flood);
        flood(entry);
        pendingTransactions.put(new HashKey(hash), new PendingTransaction(future,
                                                                          scheduler.schedule(() -> timeout(new HashKey(hash)),
                                                                                             timeout.toMillis(),
                                                                                             TimeUnit.MILLISECONDS)));
        return new HASH(hash);
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param data
     *            - the transaction content
     * @param timeout
     *            -how long to wait for finalization of the transaction
     * @return a CompleteableFuture indicating whether the transaction is finalized or not, or whether an exception
     *         occurred that prevented processing. The returned HashKey is the hash key of the the finalized transaction
     *         in the DAG
     */
    public CompletableFuture<HashKey> submitTransaction(HASH description, byte[] data, Duration timeout,
            ScheduledExecutorService scheduler) {
        CompletableFuture<HashKey> futureSailor = new CompletableFuture<>();
        submitTransaction(description, data, timeout, futureSailor, scheduler);
        return futureSailor;
    }

    /**
     * Fetch any wanted DAG entries from successors
     */
    void fetchDAG() {
        for (Ring ring : view.getRings()) {
            log.trace("Fetching DAG holes on ring: {}", ring.getIndex());
            Member successor = ring.successor(view.getNode(), m -> m.isLive());
            if (successor == null) {
                break;
            }
            List<HASH> wanted = roundPool
                                         .transactionResult(config -> dag.getWanted(parameters.limit,
                                                                                    DSL.using(config)));
            if (wanted.isEmpty()) {
                log.trace("No DAG holes on: {}", view.getNode().getId());
                return;
            }
            List<Entry> received;
            AvalancheClientCommunications connection = null;
            try {
                connection = comm.connectToNode(successor, view.getNode());
                if (connection == null) {
                    log.debug("Cannot connect wth {} for DAG gossip", successor);
                    return;
                }
                received = connection.requestDAG(wanted);
            } catch (AvroRemoteException e) {
                log.debug("comm error in DAG gossip with {}: {}", successor, e);
                return;
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            log.trace("wanted {} received {} entries", wanted.size(), received.size());
            received.forEach(entry -> {
                DagEntry dagEntry = manifestDag(entry);
                insertions.add(new HASH(hashOf(entry)), dagEntry, entry, null, dagEntry.getDescription() == null,
                               round.get() + rounds2Flood);
            });
        }

    }

    /**
     * Broadcast the transaction to all members
     * 
     * @param transaction
     */
    void flood(DagEntry transaction) {
        Entry entry = serialize(transaction);
        flood(entry);
    }

    /**
     * Broadcast the entry to all members
     * 
     * @param entry
     */
    void flood(Entry entry) {
        view.publish(AVALANCHE_TRANSACTION_CHANNEL, entry.getData().array());
    }

    /**
     * Periodically issue no op transactions for the neglected noOp transactions
     */
    void generateNoOpTxns() {
        log.trace("generating NoOp transactions");
        Set<HashKey> selected = new TreeSet<>();

        selected.addAll(dag.getNeglectedFrontier(submitPool));
        // selected.addAll(dag.getNeglectedNoOps(create));
        if (selected.isEmpty()) {
            selected.addAll(dag.getNeglected(submitPool));
        }
        if (selected.isEmpty()) { return; }
        List<HASH> pList = selected.stream().map(k -> k.toHash()).collect(Collectors.toList());
        for (int i = 0; i < parameters.noOpsPerRound; i++) {
            DagEntry dagEntry = new DagEntry();
            byte[] dummy = new byte[4];
            getEntropy().nextBytes(dummy);
            dagEntry.setData(ByteBuffer.wrap(dummy));
            dagEntry.setLinks(pList);
            Entry entry = serialize(dagEntry);
            insertions.add(new HASH(hashOf(entry)), dagEntry, entry, null, true, round.get() + rounds2Flood);
            flood(entry);
            if (log.isTraceEnabled()) {
                log.trace("noOp transaction {}",
                          pList.stream().map(e -> new HashKey(e)).collect(Collectors.toList()));
            }
        }
    }

    /**
     * for test access
     */
    ConcurrentMap<HashKey, PendingTransaction> getPendingTransactions() {
        return pendingTransactions;
    }

    /**
     * Query a random sample of ye members for their votes on the next batch of unqueried transactions for this node,
     * determine confidence from the results of the query results.
     */
    int query() {
        long now = System.currentTimeMillis();
        List<HASH> query = roundPool
                                    .transactionResult(config -> dag.query(parameters.limit, DSL.using(config),
                                                                           round.get()));
        if (query.isEmpty()) {
            log.trace("no queriable txns");
            return 0;
        }
        long retrieveTime = System.currentTimeMillis();
        log.trace("querying {} user txns in {} ms", query.size(), retrieveTime);
        Collection<Member> sample = sampler.sample(parameters.k);
        long sampleTime = System.currentTimeMillis() - retrieveTime;
        List<Boolean> results = query(query, sample);
        long queryResults = System.currentTimeMillis() - retrieveTime;
        ForkJoinPool.commonPool().execute(() -> {
            for (int i = 0; i < results.size(); i++) {
                Boolean result = results.get(i);
                if (result != null && result) {
                    preferings.add(query.get(i));
                }
            }
        });
        log.debug("querying {} user txns in {} ms ({} Q) ({} S) {{} N}", query.size(), System.currentTimeMillis() - now,
                  sampleTime, retrieveTime - now, queryResults);
        return results.size();
    }

    /**
     * Query the sample of members for their boolean opinion on the query of transaction entries
     * 
     * @param batch
     *            - the batch of unqueried nodes
     * @param sample
     *            - the random sample of members to query
     * @return for each sampled member, the list of query results for the transaction batch, or NULL if there could not
     *         be a determination (such as comm failure) of the query for that member
     */
    List<Boolean> query(List<HASH> batch, Collection<Member> sample) {
        long now = System.currentTimeMillis();
        AtomicInteger[] votes = new AtomicInteger[batch.size()];
        for (int i = 0; i < votes.length; i++) {
            votes[i] = new AtomicInteger();
        }
        // List<HASH> wanted = dag.getWanted(parameters.limit, roundPool);
        List<HASH> wanted = Collections.emptyList();

        log.debug("get wanted {} in {} ms", wanted.size(), System.currentTimeMillis() - now);

        CompletionService<Boolean> frist = new ExecutorCompletionService<>(ForkJoinPool.commonPool());
        List<Future<Boolean>> futures;

        futures = sample.stream().map(m -> frist.submit(() -> {
            QueryResult result;
            AvalancheClientCommunications connection = comm.connectToNode(m, getNode());
            if (connection == null) { return false; }
            try {
                result = connection.query(batch, wanted);
            } catch (AvroRemoteException e) {
                log.debug("Error querying {} for {}", m, batch, e);
                return false;
            } finally {
                connection.close();
            }
            log.trace("queried: {} for: {} result: {}", m, batch, result.getResult());
            if (!wanted.isEmpty()) {
                log.trace("wanted {} received {} entries", wanted.size(), result.getEntries().size());
            }

            result.getEntries().forEach(entry -> {
                DagEntry dagEntry = manifestDag(entry);
                insertions.add(new HASH(hashOf(entry)), dagEntry, entry, null, dagEntry.getDescription() == null,
                               round.get());
            });
            if (result.getResult().isEmpty()) { return false; }
            for (int i = 0; i < batch.size(); i++) {
                if (result.getResult().get(i)) {
                    votes[i].incrementAndGet();
                }
            }
            return true;
        })).collect(Collectors.toList());

        long remainingTimout = parameters.unit.toMillis(parameters.timeout);

        roundPool.transaction(config -> {
            dag.markQueried(batch.stream().map(k -> k.bytes()).collect(Collectors.toList()), DSL.using(config));
        });

        AtomicInteger responded = new AtomicInteger();
        try {
            while (responded.get() < futures.size() && remainingTimout > 0) {
                long then = System.currentTimeMillis();
                Future<Boolean> result = null;
                try {
                    result = frist.poll(remainingTimout, TimeUnit.MILLISECONDS);
                    if (result != null) {
                        responded.incrementAndGet();
                        try {
                            result.get();
                        } catch (ExecutionException e) {
                            log.debug("exception querying {}", batch, e.getCause());
                        }
                    }
                } catch (InterruptedException e) {}
                remainingTimout = remainingTimout - (System.currentTimeMillis() - then);
            }
        } finally {
            futures.forEach(f -> f.cancel(true));
        }
        List<Boolean> queryResults = new ArrayList<>();
        for (int i = 0; i < batch.size(); i++) {
            queryResults.add(votes[i].get() > required);
        }

        log.debug("query results: {} in: {} ms", queryResults.size(), System.currentTimeMillis() - now);
        return queryResults;
    }

    void round() {
        log.trace("Performing round");
        try {
            for (int i = 0; i < parameters.gamma; i++) {
                query();
            }
        } catch (Throwable t) {
            log.error("Error performing Avalanche batch round", t);
        }
    }

    /**
     * Timeout the pending transaction
     * 
     * @param key
     */
    void timeout(HashKey key) {
        PendingTransaction pending = pendingTransactions.remove(key);
        if (pending == null) { return; }
        pending.timer.cancel(true);
        pending.pending.complete(null);
    }

    private void finalizerLoop() {
        DSLContext context;
        try {
            context = DSL.using(DriverManager.getConnection(parameters.dbConnect, USER_NAME, PASSWORD), SQLDialect.H2);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        while (running.get()) {
            try {
                if (!running.get()) { return; }
                nextPreferred(context);
                if (!running.get()) { return; }
                nextFinalized(context);
            } catch (Throwable e) {
                if (e.getCause() instanceof JdbcSQLTransactionRollbackException) {
                    log.info("mutator rolled back: {}", e);
                } else {
                    log.error("error in mutator", e);
                    context.close();
                    try {
                        context = DSL.using(DriverManager.getConnection(parameters.dbConnect, USER_NAME, PASSWORD),
                                            SQLDialect.H2);
                    } catch (SQLException s) {
                        throw new IllegalStateException(s);
                    }
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {}
        }
    }

    private SecureRandom getEntropy() {
        return getNode().getParameters().entropy;
    }

    @SuppressWarnings("unused")
    private void getWanted() {
        Member next = sampler.next();
        if (next == null) { return; }

        List<HASH> want = roundPool.transactionResult(config -> dag.getWanted(parameters.limit, DSL.using(config)));
        AvalancheClientCommunications connection = comm.connectToNode(next, getNode());
        if (connection == null) { return; }
        List<Entry> requested;
        try {
            requested = connection.requestDAG(want);
        } catch (AvroRemoteException e) {
            log.info("error getting wanted from {} ", next);
            return;
        }
        if (requested.isEmpty()) { return; }
        requested.forEach(entry -> {
            DagEntry dagEntry = manifestDag(entry);
            insertions.add(new HASH(hashOf(entry)), dagEntry, entry, null, dagEntry.getDescription() == null,
                           round.get());
        });
    }

    private void initializeProcessors(ClassLoader resolver) {
        queryPool.transaction(config -> {
            DSLContext context = DSL.using(config);
            context.mergeInto(PROCESSORS)
                   .columns(PROCESSORS.HASH, PROCESSORS.PROCESSORCLASS)
                   .values(WellKnownDescriptions.GENESIS.toHash().bytes(), "<Genesis Handler>");
            context.mergeInto(PROCESSORS)
                   .columns(PROCESSORS.HASH, PROCESSORS.PROCESSORCLASS)
                   .values(WellKnownDescriptions.BYTE_CONTENT.toHash().bytes(), "<Unconstrainted Byte content>");
        });
        queryPool.select(PROCESSORS.HASH, PROCESSORS.PROCESSORCLASS).from(PROCESSORS).stream().forEach(r -> {
            processors.computeIfAbsent(new HashKey(r.value1()), k -> resolve(r.value2(), resolver));
        });
    }

    private void insertLoop() {
        DSLContext context;
        try {
            context = DSL.using(DriverManager.getConnection(parameters.dbConnect, USER_NAME, PASSWORD), SQLDialect.H2);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        while (running.get()) {
            try {
                if (!running.get()) { return; }
                nextInserts(context);
            } catch (Throwable e) {
                if (e.getCause() instanceof JdbcSQLTransactionRollbackException) {
                    log.info("mutator rolled back: {}", e);
                } else {
                    log.error("error in mutator", e);
                    context.close();
                    try {
                        context = DSL.using(DriverManager.getConnection(parameters.dbConnect, USER_NAME, PASSWORD),
                                            SQLDialect.H2);
                    } catch (SQLException s) {
                        throw new IllegalStateException(s);
                    }
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {}
        }
    }

    private List<byte[]> nextFinalizations(int batch) {
        List<byte[]> next = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            HASH key;
            try {
                key = finalizing.poll(i == 0 ? 10 : 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                break;
            }
            if (key != null) {
                next.add(key.bytes());
            } else {
                break;
            }
        }
        return next;
    }

    private void nextFinalized(DSLContext context) {
        List<byte[]> batch = nextFinalizations(parameters.finalizeBatchSize);
        if (batch.isEmpty()) { return; }
        FinalizationData d = context.transactionResult(config -> dag.tryFinalize(batch, DSL.using(config)));
        finalized.addAndGet(d.finalized.size());
        ForkJoinPool.commonPool().execute(() -> {
            d.finalized.forEach(key -> {
                PendingTransaction pending = pendingTransactions.remove(key);
                if (pending != null) {
                    pending.complete(key);
                }
            });

            d.deleted.forEach(key -> {
                PendingTransaction pending = pendingTransactions.remove(key);
                if (pending != null) {
                    pending.complete(null);
                }
            });
            log.trace("Finalizing: {}, deleting: {}", d.finalized, d.deleted);
        });
    }

    private void nextInserts(DSLContext context) {
        List<DagInsert> next = insertions.next(parameters.insertBatchSize);
        if (next.isEmpty()) { return; }
        context.transaction(config -> {
            next.forEach(insert -> {
                dag.putDagEntry(insert.key, insert.dagEntry, insert.entry, insert.conflictSet, DSL.using(config),
                                insert.noOp, insert.targetRound);
            });
        });
    }

    private List<HASH> nextPreferences(int batch) {
        List<HASH> next = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            HASH key;
            try {
                key = preferings.poll(i == 0 ? 10 : 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                break;
            }
            if (key != null) {
                next.add(key);
            } else {
                break;
            }
        }
        return next;
    }

    private void nextPreferred(DSLContext context) {
        List<HASH> batch = nextPreferences(parameters.preferBatchSize);
        if (!batch.isEmpty()) {
            context.transaction(config -> dag.prefer(batch, DSL.using(config)));
            finalizing.addAll(batch);
        }
    }

    private EntryProcessor resolve(String processor, ClassLoader resolver) {
        try {
            return (EntryProcessor)resolver.loadClass(processor).getConstructor(new Class[0]).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            log.warn("Unresolved processor configured: {} : {}", processor, e);
            return null;
        }
    }
}
