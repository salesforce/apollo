/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import java.io.File;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.salesforce.apollo.IdentitySource.DefaultIdentitySource;
import com.salesforce.apollo.avalanche.AvalancheParameters;
import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheLocalCommSim;
import com.salesforce.apollo.avalanche.communications.netty.AvalancheNettyCommunications;
import com.salesforce.apollo.fireflies.communications.FfLocalCommSim;
import com.salesforce.apollo.fireflies.communications.FirefliesCommunications;
import com.salesforce.apollo.fireflies.communications.netty.FirefliesNettyCommunications;
import com.salesforce.apollo.ghost.Ghost.GhostParameters;
import com.salesforce.apollo.ghost.communications.GhostCommunications;
import com.salesforce.apollo.ghost.communications.GhostLocalCommSim;
import com.salesforce.apollo.ghost.communications.netty.GhostNettyCommunications;

/**
 * @author hal.hildebrand
 * @since 218
 */
public class ApolloConfiguration {

	public interface CommunicationsFactory {
		AvalancheCommunications avalanche();

		FirefliesCommunications fireflies();

		GhostCommunications ghost();
	}

	public static class FileIdentitySource implements IdentityStoreSource {

		public char[] password = DEFAULT_PASSWORD;
		public File store = new File(".keystore");
		public String type = DEFAULT_TYPE;

		@Override
		public IdentitySource getIdentitySource(String caAlias, String identityAlias) {
			try {
				return new DefaultIdentitySource(caAlias, store, type, identityAlias, password);
			} catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e) {
				throw new IllegalStateException("Cannot create file identity source", e);
			}
		}

	}

	public interface IdentityStoreSource {
		IdentitySource getIdentitySource(String caAlias, String identityAlias);
	}

	public static class NettyCommunicationsFactory implements CommunicationsFactory {

		@Override
		public AvalancheCommunications avalanche() {
			return new AvalancheNettyCommunications();
		}

		@Override
		public FirefliesCommunications fireflies() {
			return new FirefliesNettyCommunications();
		}

		@Override
		public GhostCommunications ghost() {
			return new GhostNettyCommunications();
		}

	}

	public static class ResourceIdentitySource implements IdentityStoreSource {

		public char[] password = DEFAULT_PASSWORD;
		public String store = ".keystore";
		public String type = DEFAULT_TYPE;

		@Override
		public IdentitySource getIdentitySource(String caAlias, String identityAlias) {
			try {
				return new DefaultIdentitySource(caAlias, store, type, identityAlias, password);
			} catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e) {
				throw new IllegalStateException("Cannot create resource identity source", e);
			}
		}
	}

	public static class SimCommunicationsFactory implements CommunicationsFactory {

		static {
			reset();
		}

		public static AvalancheLocalCommSim AVALANCHE_LOCAL_COMM;
		public static FfLocalCommSim FF_LOCAL_COM;
		public static GhostLocalCommSim GHOST_LOCAL_COMM;

		public static void reset() {
			AVALANCHE_LOCAL_COMM = new AvalancheLocalCommSim();
			FF_LOCAL_COM = new FfLocalCommSim();
			GHOST_LOCAL_COMM = new GhostLocalCommSim();
		}

		@Override
		public AvalancheCommunications avalanche() {
			if (AVALANCHE_LOCAL_COMM == null) {
				throw new IllegalStateException("SimCommunicationsFactory must be reset first");
			}
			return AVALANCHE_LOCAL_COMM;
		}

		@Override
		public FirefliesCommunications fireflies() {
			if (FF_LOCAL_COM == null) {
				throw new IllegalStateException("SimCommunicationsFactory must be reset first");
			}
			return FF_LOCAL_COM;
		}

		@Override
		public GhostCommunications ghost() {
			if (GHOST_LOCAL_COMM == null) {
				throw new IllegalStateException("SimCommunicationsFactory must be reset first");
			}
			return GHOST_LOCAL_COMM;
		}

	}

	public static final String DEFAULT_CA_ALIAS = "CA";
	public static final Duration DEFAULT_GOSSIP_INTERVAL = Duration.ofMillis(1_000);
	public static final String DEFAULT_IDENTITY_ALIAS = "identity";
	public static final char[] DEFAULT_PASSWORD = "".toCharArray();
	public static final String DEFAULT_TYPE = "PKCS12";

	public AvalancheParameters avalanche = new AvalancheParameters();
	public long bufferSize = 100 * 1024;
	public String ca = DEFAULT_CA_ALIAS;
	@JsonSubTypes({ @Type(value = SimCommunicationsFactory.class, name = "sim"),
			@Type(value = NettyCommunicationsFactory.class, name = "netty") })
	@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
	public CommunicationsFactory communications = new NettyCommunicationsFactory();
	public GhostParameters ghost = new GhostParameters();
	public Duration gossipInterval = DEFAULT_GOSSIP_INTERVAL;
	public String identity = DEFAULT_IDENTITY_ALIAS;
	@JsonSubTypes({ @Type(value = FileIdentitySource.class, name = "file"),
			@Type(value = ResourceIdentitySource.class, name = "resource"),
			@Type(value = BootstrapIdSource.class, name = "bootstrap"),
			@Type(value = EnvironmentConfiguredIdSource.class, name = "env") })
	@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
	public IdentityStoreSource source;
	public int threadPool = 10;
}
