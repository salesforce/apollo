/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.View.isValidMask;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The representation of the "current" member - the subject - of a View.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Node extends Member {
	public class NodeKeyManagerFactory extends KeyManagerFactory {

		public NodeKeyManagerFactory() {
			super(new NodeKeyManagerFactorySpi(), PROVIDER, getId() + " Keys");
		}

	}

	public class NodeKeyManagerFactorySpi extends KeyManagerFactorySpi {

		@Override
		protected KeyManager[] engineGetKeyManagers() {
			return new KeyManager[] { new Keys() };
		}

		@Override
		protected void engineInit(KeyStore ks, char[] password)
				throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException {
		}

		@Override
		protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {
		}

	}

	public class NodeTrustManagerFactory extends TrustManagerFactory {

		public NodeTrustManagerFactory() {
			super(new NodeTrustManagerFactorySpi(), PROVIDER, getId() + " Trust");
		}

	}

	public class NodeTrustManagerFactorySpi extends TrustManagerFactorySpi {

		@Override
		protected TrustManager[] engineGetTrustManagers() {
			return new TrustManager[] { new Trust() };
		}

		@Override
		protected void engineInit(KeyStore ks) throws KeyStoreException {
		}

		@Override
		protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {
		}

	}

	private class Keys extends X509ExtendedKeyManager {
		private String alias = getId().toString();

		@Override
		public String chooseClientAlias(String[] keyType, Principal[] principals, Socket socket) {
			return alias;
		}

		@Override
		public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
			return alias;
		}

		@Override
		public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
			return alias;
		}

		@Override
		public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
			return alias;
		}

		@Override
		public X509Certificate[] getCertificateChain(String s) {
			return new X509Certificate[] { getCertificate() };
		}

		@Override
		public String[] getClientAliases(String keyType, Principal[] principals) {
			return new String[] { alias };
		}

		@Override
		public PrivateKey getPrivateKey(String alias) {
			if (this.alias.equals(alias)) {
				return privateKey;
			}
			return null;
		}

		@Override
		public String[] getServerAliases(String s, Principal[] principals) {
			return new String[] { alias };
		}
	}

	private class Trust extends X509ExtendedTrustManager {

		@Override
		public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
			chain[0].checkValidity();
			try {
				chain[0].verify(parameters.ca.getPublicKey());
			} catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | CertificateException
					| NoSuchProviderException e) {
				throw new CertificateException("Invalid cert: " + chain[0].getSubjectDN());
			}
		}

		@Override
		public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
				throws CertificateException {
			// TODO Auto-generated method stub

		}

		@Override
		public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
				throws CertificateException {
			// TODO Auto-generated method stub

		}

		@Override
		public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
			chain[0].checkValidity();
			try {
				chain[0].verify(parameters.ca.getPublicKey());
			} catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | CertificateException
					| NoSuchProviderException e) {
				throw new CertificateException("Invalid cert: " + chain[0].getSubjectDN());
			}
		}

		@Override
		public void checkServerTrusted(X509Certificate[] arg0, String arg1, Socket arg2) throws CertificateException {
			// TODO Auto-generated method stub

		}

		@Override
		public void checkServerTrusted(X509Certificate[] arg0, String arg1, SSLEngine arg2)
				throws CertificateException {
			// TODO Auto-generated method stub

		}

		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return new X509Certificate[] { parameters.ca };
		}

	}

	public static final Provider PROVIDER = new BouncyCastleProvider();

	private static final Logger log = LoggerFactory.getLogger(Node.class);

	static {
		Security.addProvider(PROVIDER);
	}
	static {
		javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(new javax.net.ssl.HostnameVerifier() {

			public boolean verify(String hostname, javax.net.ssl.SSLSession sslSession) {
				return true;
			}
		});
	}

	/**
	 * Create a mask of length 2t+1 with t randomly disabled rings
	 * 
	 * @param toleranceLevel - t
	 * @return the mask
	 */
	public static BitSet createInitialMask(int toleranceLevel, Random entropy) {
		int nbits = 2 * toleranceLevel + 1;
		BitSet mask = new BitSet(nbits);
		List<Boolean> random = new ArrayList<>();
		for (int i = 0; i < toleranceLevel + 1; i++) {
			random.add(true);
		}
		for (int i = 0; i < toleranceLevel; i++) {
			random.add(false);
		}
		Collections.shuffle(random, entropy);
		for (int i = 0; i < nbits; i++) {
			if (random.get(i)) {
				mask.set(i);
			}
		}
		return mask;
	}

	/**
	 * The node's signing key
	 */
	protected final PrivateKey privateKey;

	/**
	 * Ye params
	 */
	private final FirefliesParameters parameters;

    private final SSLContext sslContext;

	public Node(CertWithKey identity, FirefliesParameters p) {
		this(identity, p, portsFrom(identity.getCertificate()));
	}

    public Node(CertWithKey identity, FirefliesParameters parameters, InetSocketAddress[] boundPorts) {
		super(identity.getCertificate(), null, parameters, null, boundPorts);

		privateKey = identity.getPrivateKey();
		this.parameters = parameters;

        try {
            sslContext = SSLContext.getInstance("TLS");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Cannot get SSL Context instance 'TLS'", e);
        }
        try {
            sslContext.init(new KeyManager[] { new Keys() }, new TrustManager[] { new Trust() }, parameters.entropy);
        } catch (KeyManagementException e) {
            throw new IllegalStateException("Cannot get SSL Context instance 'TLS'", e);
        }

		log.info("Node[{}] ports: [FF:{}, G:{}, A:{}]", getId(), getFirefliesEndpoint(), getGhostEndpoint(),
				getAvalancheEndpoint());
	}

	public X509Certificate getCA() {
		return parameters.ca;
	}

	public X509KeyManager getKeyManager() {
		return new Keys();
	}

	public KeyManagerFactory getKeyManagerFactory() {
		return new NodeKeyManagerFactory();
	}

	/**
	 * @return the configuration parameters for this node
	 */
	public FirefliesParameters getParameters() {
		return parameters;
	}

	public SSLContext getSslContext() {
        return sslContext;
    }

	public X509TrustManager getTrustManager() {
		return new Trust();
	}

	public TrustManagerFactory getTrustManagerFactory() {
		return new NodeTrustManagerFactory();
	}

	public void isDisabled(Ring ring) {
		getNote();
	}

	@Override
	public String toString() {
		return "Node[" + getId() + "]";
	}

	Accusation accuse(Member m, int ringNumber) {
		return new Accusation(m.getEpoch(), getId(), ringNumber, m.getId(), forSigning());
	}

	Signature forSigning() {
		Signature signature;
		try {
			signature = Signature.getInstance(parameters.signatureAlgorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("no such algorithm: " + parameters.signatureAlgorithm, e);
		}
		try {
			signature.initSign(privateKey, parameters.entropy);
		} catch (InvalidKeyException e) {
			throw new IllegalStateException("invalid private key", e);
		}
		return signature;
	}

	Signature forVerification() {
		return forVerification(parameters.signatureAlgorithm);
	}

	/**
	 * @return a new mask based on the previous mask and previous accusations.
	 */
	BitSet nextMask() {
		Note current = note;
		if (current == null) {
			BitSet mask = createInitialMask(parameters.toleranceLevel, parameters.entropy);
			assert View.isValidMask(mask, parameters) : "Invalid initial mask: " + mask + "for node: " + getId();
			return mask;
		}

		BitSet mask = new BitSet(parameters.rings);
		mask.flip(0, parameters.rings);
		for (int i : validAccusations.keySet()) {
			if (mask.cardinality() <= parameters.toleranceLevel + 1) {
				assert isValidMask(mask, parameters) : "Invalid mask: " + mask + "for node: " + getId();
				return mask;
			}
			mask.set(i, false);
		}
		if (current.getEpoch() % 2 == 1) {
			BitSet previous = current.getMask();
			for (int index = 0; index < parameters.rings; index++) {
				if (mask.cardinality() <= parameters.toleranceLevel + 1) {
					assert View.isValidMask(mask, parameters) : "Invalid mask: " + mask + "for node: " + getId();
					return mask;
				}
				if (!previous.get(index)) {
					mask.set(index, false);
				}
			}
		} else {
			// Fill the rest of the mask with randomly set index
			while (mask.cardinality() > parameters.toleranceLevel + 1) {
				int index = parameters.entropy.nextInt(parameters.rings);
				if (mask.get(index)) {
					mask.set(index, false);
				}
			}
		}
		assert isValidMask(mask, parameters) : "Invalid mask: " + mask + "for node: " + getId();
		return mask;
	}

	/**
	 * Generate a new note for the member based on any previous note and previous
	 * accusations. The new note has a larger epoch number the the current note.
	 */
	void nextNote() {
		Note current = note;
		long newEpoch = current == null ? 1 : note.getEpoch() + 1;
		nextNote(newEpoch);
	}

	/**
	 * Generate a new note using the new epoch
	 * 
	 * @param newEpoch
	 */
	void nextNote(long newEpoch) {
		note = new Note(getId(), newEpoch, nextMask(), forSigning());
	}
}
