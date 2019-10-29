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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConnectionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.dagwood.schema.tables.records.DagRecord;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class DagTest {

	private static final String CONNECTION_URL = "jdbc:h2:mem:test";
	private Connection          connection;
	private DSLContext          create;
	private Dag                 dag;
	private Random              entropy;
	private DagEntry            root;
	private HashKey             rootKey;

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
		Avalanche.loadSchema(CONNECTION_URL);
		connection = DriverManager.getConnection(CONNECTION_URL, "apollo", "");
		connection.setAutoCommit(false);
		ConnectionProvider provider = new DefaultConnectionProvider(connection);
		create = DSL.using(provider, SQLDialect.H2);
		create.deleteFrom(DAG).execute();
		create.deleteFrom(CLOSURE).execute();
		create.deleteFrom(CONFLICTSET).execute();
		entropy = new Random(0x666);
		dag = new Dag(null);
		root = new DagEntry();
		root.setDescription(WellKnownDescriptions.GENESIS.toHash());
		root.setData(ByteBuffer.wrap("Ye root".getBytes()));
		rootKey = new HashKey(dag.putDagEntry(root, serialize(root), null, create, false, 0));
		assertNotNull(rootKey);
	}

	@Test
	@Ignore // note we do lazy DAG closure GC now. Need to change this test - HSH
	public void maintenance() throws Exception {
		List<HASH> ordered = new ArrayList<>();

		Map<HASH, DagEntry> stored = new ConcurrentSkipListMap<>();
		stored.put(rootKey.toHash(), root);
		ordered.add(rootKey.toHash());

		DagEntry entry = new DagEntry();
		entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
		entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 1).getBytes()));
		entry.setLinks(asList(rootKey.toHash()));
		HASH key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
		stored.put(key, entry);
		ordered.add(key);

		entry = new DagEntry();
		entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
		entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 2).getBytes()));
		entry.setLinks(asList(key));
		key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
		stored.put(key, entry);
		ordered.add(key);

		entry = new DagEntry();
		entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
		entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 3).getBytes()));
		entry.setLinks(asList(ordered.get(1), ordered.get(2)));
		key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
		stored.put(key, entry);
		ordered.add(key);

		List<HASH> closure = dag.closure(ordered.get(3), create).collect(Collectors.toList());
		DagViz.dumpClosures(ordered, create);
		assertEquals(3, closure.size());
	}

	@Test
	public void smoke() throws Exception {
		DagEntry testRoot = dag.getDagEntry(rootKey.toHash(), create);
		assertNotNull(testRoot);
		testRoot.setDescription(WellKnownDescriptions.GENESIS.toHash());
		assertNotNull(testRoot);
		assertArrayEquals(root.getData().array(), testRoot.getData().array());
		assertNull(testRoot.getLinks());

		List<HASH> ordered = new ArrayList<>();
		ordered.add(rootKey.toHash());

		Map<HASH, DagEntry> stored = new ConcurrentSkipListMap<>();
		stored.put(rootKey.toHash(), root);

		for (int i = 0; i < 500; i++) {
			DagEntry entry = new DagEntry();
			entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
			entry.setData(ByteBuffer.wrap(String.format("Entry: %s", i).getBytes()));
			entry.setLinks(randomLinksTo(stored));
			HASH key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
			stored.put(key, entry);
			ordered.add(key);
		}
		assertEquals(501, stored.size());

		Result<DagRecord> records = create.selectFrom(DAG).fetch();
		assertEquals(501, records.size());

		for (HASH key : ordered) {
			assertEquals(Integer.valueOf(1), create.select(CONFLICTSET.CARDINALITY).from(CONFLICTSET).join(UNFINALIZED)
					.on(UNFINALIZED.CONFLICTSET.eq(CONFLICTSET.NODE)).and(DAG.KEY.eq(key.bytes())).fetchOne().value1());
			DagEntry found = dag.getDagEntry(key, create);
			assertNotNull("Not found: " + key, found);
			DagEntry original = stored.get(key);
			assertArrayEquals(original.getData().array(), found.getData().array());
			if (original.getLinks() == null) {
				assertNull(found.getLinks());
			} else {
				assertEquals(original.getLinks().size(), found.getLinks().size());
			}
			List<HASH> retrieved = dag.closure(key, create).collect(Collectors.toList());
			assertNotNull(retrieved);
			assertEquals(new ConcurrentSkipListSet<>(retrieved).size(), retrieved.size());
		}
	}

	@Test
	public void wanted() throws Exception {
		List<HASH> ordered = new ArrayList<>();

		Map<HASH, DagEntry> stored = new ConcurrentSkipListMap<>();
		stored.put(rootKey.toHash(), root);
		ordered.add(rootKey.toHash());

		DagEntry entry = new DagEntry();
		entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
		entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 1).getBytes()));
		entry.setLinks(asList(rootKey.toHash()
		                      ));
		HASH key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
		stored.put(key, entry);
		ordered.add(key);

		entry = new DagEntry();
		entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
		entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 2).getBytes()));
		entry.setLinks(asList(key));
		key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
		stored.put(key, entry);
		ordered.add(key);

		entry = new DagEntry();
		entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
		entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 3).getBytes()));
		HASH hash = new HASH(new byte[32]);
		entry.setLinks(asList(key, hash));
		key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
		stored.put(key, entry);
		ordered.add(key);

		List<HASH> wanted = dag.getWanted(100, create);
		assertNotNull(wanted);
		assertEquals(1, wanted.size());
		assertArrayEquals(new byte[32], wanted.get(0).bytes());

		entry = new DagEntry();
		entry.setDescription(WellKnownDescriptions.BYTE_CONTENT.toHash());
		entry.setData(ByteBuffer.wrap(String.format("Entry: %s", 4).getBytes()));
		entry.setLinks(asList(key, hash));
		key = dag.putDagEntry(entry, serialize(entry), null, create, false, 0);
		stored.put(key, entry);
		ordered.add(key);

		wanted = dag.getWanted(100, create);
		assertNotNull(wanted);
		assertEquals(1, wanted.size());
		assertArrayEquals(new byte[32], wanted.get(0).bytes());
	}

	private List<HASH> randomLinksTo(Map<HASH, DagEntry> stored) {
		List<HASH> links = new ArrayList<>();
		Set<HASH> keys = stored.keySet();
		for (int i = 0; i < 5; i++) {
			Iterator<HASH> it = keys.iterator();
			for (int j = 0; j < entropy.nextInt(keys.size()); j++) {
				it.next();
			}
			links.add(it.next());
		}
		return links;
	}
}
