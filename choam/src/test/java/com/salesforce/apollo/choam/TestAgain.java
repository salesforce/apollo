/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reassemble2;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class TestAgain {

	private static short CARDINALITY = 4;
	private Digest nextViewId;
	private CountDownLatch complete;

	@BeforeEach
	public void before() throws Exception {

		var entropy = SecureRandom.getInstance("SHA1PRNG");
		entropy.setSeed(new byte[] { 6, 6, 6 });
		var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

		members = IntStream.range(0, CARDINALITY).mapToObj(i -> stereotomy.newIdentifier().get())
				.map(cpk -> new ControlledIdentifierMember(cpk)).map(e -> (SigningMember) e).toList();

		context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(2), members.size(), 0.1, 3);
		for (Member m : members) {
			context.activate(m);
		}
		nextViewId = context.getId().prefix(0x666);

		dataSources = members.stream().collect(Collectors.toMap(m -> m, m -> new VDataSource()));
		complete = new CountDownLatch(context.activeCount());
	}

	@AfterEach
	public void after() {
		controllers.forEach(e -> e.stop());
		gossipers.forEach(e -> e.stop());
		communications.values().forEach(e -> e.close());
	}

	private static class VDataSource implements DataSource {

		private BlockingQueue<Reassemble2> outbound = new ArrayBlockingQueue<>(100);

		@Override
		public ByteString getData() {
			Reassemble2.Builder result;
			try {
				result = Reassemble2.newBuilder(outbound.poll(100, TimeUnit.MILLISECONDS));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return ByteString.EMPTY;
			}
			while (outbound.peek() != null) {
				var current = outbound.peek();
				result.addAllMembers(current.getMembersList()).addAllValidations(current.getValidationsList());
			}
			return result.build().toByteString();
		}

		public void publish(Reassemble2 r) {
		}

	}

	private void initEthereals() {
		var builder = Config.newBuilder().setFpr(0.000125).setnProc(CARDINALITY)
				.setVerifiers(members.toArray(new Verifier[members.size()]));

		final var prefix = UUID.randomUUID().toString();
		for (short i = 0; i < CARDINALITY; i++) {
			final short pid = i;
			var execN = new AtomicInteger();
			var executor = Executors.newFixedThreadPool(2,
					r -> new Thread(r, "system executor: " + execN.incrementAndGet() + " for: " + pid));
			var com = new LocalRouter(prefix, ServerConnectionCache.newBuilder(), executor, null);
			communications.put(members.get(i), com);
			final var member = members.get(i);
			var controller = new Ethereal(builder.setSigner(members.get(i)).setPid(pid).build(), 1024 * 1024,
					dataSources.get(member), (pb, last) -> {
					}, ep -> {
					});

			var gossiper = new ChRbcGossip(context, member, controller.processor(), com, executor, null);
			gossipers.add(gossiper);
			controllers.add(controller);
			com.setMember(members.get(i));
		}
	}

	private void buildAssemblies() {
		Parameters.Builder params = Parameters.newBuilder()
				.setProducer(ProducerParameters.newBuilder().setGossipDuration(Duration.ofMillis(10)).build())
				.setGossipDuration(Duration.ofMillis(10));
		Map<Member, ViewAssembly2> assemblies = new HashMap<>();
		Map<Member, Concierge> servers = members.stream().collect(Collectors.toMap(m -> m, m -> mock(Concierge.class)));
		Map<Member, KeyPair> consensusPairs = new HashMap<>();
		servers.forEach((m, s) -> {
			KeyPair keyPair = params.getViewSigAlgorithm().generateKeyPair();
			consensusPairs.put(m, keyPair);
			final PubKey consensus = bs(keyPair.getPublic());
			when(s.join(any(JoinRequest.class), any(Digest.class))).then(new Answer<ViewMember>() {
				@Override
				public ViewMember answer(InvocationOnMock invocation) throws Throwable {
					return ViewMember.newBuilder().setId(m.getId().toDigeste()).setConsensusKey(consensus)
							.setSignature(((Signer) m).sign(consensus.toByteString()).toSig()).build();
				}
			});
		});
		var comms = members.stream()
				.collect(Collectors.toMap(m -> m,
						m -> communications.get(m).create(m, context.getId(), servers.get(m),
								r -> new TerminalServer(comms.get(m).getClientIdentityProvider(), null, r,
										Executors.newSingleThreadExecutor()),
								TerminalClient.getCreate(null),
								Terminal.getLocalLoopback((SigningMember) m, servers.get(m)))));

		Map<Member, Verifier> validators = consensusPairs.entrySet().stream().collect(
				Collectors.toMap(e -> e.getKey(), e -> new Verifier.DefaultVerifier(e.getValue().getPublic())));
		Map<Member, VDataSource> dataSources = members.stream()
				.collect(Collectors.toMap(m -> m, m -> new VDataSource()));
		Map<Member, ViewContext> views = new HashMap<>();
		context.active().forEach(m -> {
			SigningMember sm = (SigningMember) m;
			Router router = communications.get(m);
			ViewContext view = new ViewContext(context,
					params.build(RuntimeParameters.newBuilder().setExec(Executors.newFixedThreadPool(2))
							.setScheduler(Executors.newSingleThreadScheduledExecutor()).setContext(context)
							.setMember(sm).setCommunications(router).build()),
					new Signer.SignerImpl(consensusPairs.get(m).getPrivate()), validators, null);
			views.put(m, view);
			assemblies.put(m, new ViewAssembly2(nextViewId, view, r -> dataSources.get(m).publish(r), comms.get(m)) {
				@Override
				public void complete() {
					super.complete();
					complete.countDown();
				}
			});
		});
	}

	private List<SigningMember> members;
	private Context<Member> context;
	private List<Ethereal> controllers = new ArrayList<>();
	private Map<Member, VDataSource> dataSources;
	private List<ChRbcGossip> gossipers = new ArrayList<>();
	private Map<Member, Router> communications = new HashMap<>();

	public void testIt() throws Exception {

		final var gossipPeriod = Duration.ofMillis(5);

		controllers.forEach(e -> e.start());
		communications.values().forEach(e -> e.start());
		gossipers.forEach(e -> e.start(gossipPeriod, Executors.newSingleThreadScheduledExecutor()));
	}
}
