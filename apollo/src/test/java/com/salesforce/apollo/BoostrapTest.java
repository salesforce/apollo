/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.bootstrap.BootstrapCA;
import com.salesforce.apollo.bootstrap.BootstrapConfiguration;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

/**
 * @author hhildebrand
 */
public class BoostrapTest {

	@ClassRule
	public static final DropwizardAppRule<BootstrapConfiguration> RULE = new DropwizardAppRule<BootstrapConfiguration>(
			BootstrapCA.class, ResourceHelpers.resourceFilePath("bootstrap.yml")) {

		@Override
		protected JerseyClientBuilder clientBuilder() {
			return super.clientBuilder().property(ClientProperties.CONNECT_TIMEOUT, 1000)
					.property(ClientProperties.READ_TIMEOUT, 60_000);
		}
	};

	@Test
	public void smoke() throws Exception {
		ApolloConfiguration.SimCommunicationsFactory.reset();
		List<Apollo> oracles = new ArrayList<>();
		URL endpoint = new URL(String.format("http://localhost:%d/api/cnc/mint", RULE.getLocalPort()));

		for (int i = 1; i <= PregenPopulation.getCardinality(); i++) {
			ApolloConfiguration config = new ApolloConfiguration();
			config.avalanche.alpha = 0.6;
			config.avalanche.k = 6;
			config.avalanche.beta1 = 3;
			config.avalanche.beta2 = 5;
			config.avalanche.dbConnect = "jdbc:h2:mem:bootstrap-" + i;
			config.gossipInterval = Duration.ofMillis(500);
			config.communications = new ApolloConfiguration.SimCommunicationsFactory();
			BootstrapIdSource ks = new BootstrapIdSource();
			ks.endpoint = endpoint;
			config.source = ks;
			oracles.add(new Apollo(config));
		}
		long then = System.currentTimeMillis();

		oracles.forEach(oracle -> {
			try {
				oracle.start();
			} catch (Exception e) {
				throw new IllegalStateException("unable to start oracle", e);
			}
		});

		assertTrue("Did not stabilize the view", Utils.waitForCondition(15_000, 1_000, () -> {
			return oracles.stream().map(o -> o.getView())
					.map(view -> view.getLive().size() != oracles.size() ? view : null).filter(view -> view != null)
					.count() == 0;
		}));

		System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
				+ oracles.size() + " members");
		Avalanche master = oracles.get(0).getAvalanche();
		System.out.println("Start round: " + master.getRoundCounter());
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		CompletableFuture<HashKey> genesis = master.createGenesis("Genesis".getBytes(), Duration.ofSeconds(90),
				scheduler);
		HashKey genesisKey = null;
		try {
			genesisKey = genesis.get(60, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			oracles.forEach(node -> node.stop());
		}
		System.out.println("Rounds: " + master.getRoundCounter());
		assertNotNull(genesisKey);

		long now = System.currentTimeMillis();
		List<Transactioneer> transactioneers = new ArrayList<>();
		HASH k = genesisKey.toHash();
		for (Apollo o : oracles) {
			assertTrue("Failed to finalize genesis on: " + o.getAvalanche().getNode().getId(),
					Utils.waitForCondition(15_000, () -> o.getAvalanche().getDagDao().isFinalized(k)));
			transactioneers.add(new Transactioneer(o.getAvalanche()));
		}

		// # of txns per node
		int target = 15;
		transactioneers.forEach(t -> t.transact(Duration.ofSeconds(120), target * 40, scheduler));

		boolean finalized = Utils.waitForCondition(300_000, 1_000, () -> {
			return transactioneers.stream().mapToInt(t -> t.getSuccess()).filter(s -> s >= target)
					.count() == transactioneers.size();
		});

		System.out.println("Completed in " + (System.currentTimeMillis() - now) + " ms");
		transactioneers.forEach(t -> t.stop());
		oracles.forEach(node -> node.stop());
		// System.out.println(profiler.getTop(3));

		TestApollo.summarize(oracles);
		oracles.forEach(node -> TestApollo.summary(node.getAvalanche()));

		System.out.println("wanted: ");
		System.out.println(master.getDag().getWanted(Integer.MAX_VALUE, master.getDslContext()).stream()
				.map(e -> new HashKey(e)).collect(Collectors.toList()));
		System.out.println();
		System.out.println();
		assertTrue("failed to finalize " + target + " txns: " + transactioneers, finalized);
		transactioneers.forEach(t -> {
			System.out.println("failed to finalize " + t.getFailed() + " for " + t.getId());
		});

	}
}
