/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.domain;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.ObjectHandle;
import org.graalvm.nativeimage.ObjectHandles;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Foundation;
import com.salesfoce.apollo.choam.proto.FoundationSeal;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesforce.apollo.archipelago.Enclave;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.delphinius.Oracle.Assertion;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.Domain.TransactionConfiguration;
import com.salesforce.apollo.model.ProcessDomain;
import com.salesforce.apollo.model.SubDomain;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;

import io.netty.channel.unix.DomainSocketAddress;

/**
 * @author hal.hildebrand
 *
 */
public class Demesne {
    private static final int                      CARDINALITY     = 5;
    private static final AtomicReference<Demesne> demesne         = new AtomicReference<>();
    private static final Digest                   GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());
    private static final Logger                   log             = LoggerFactory.getLogger(Demesne.class);

    public static String launch(DemesneParameters parameters, char[] pwd) throws GeneralSecurityException {
        if (demesne.get() == null) {
            return null;
        }
        final var pretending = new Demesne(parameters, pwd);
        if (!demesne.compareAndSet(null, pretending)) {
            return null;
        }
        return pretending.getInbound();
    }

    public static void main(String[] argv) {
        try {
            final var demesne = new Demesne();
            demesne.before();
            demesne.smokin();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    @SuppressWarnings("unused")
    public static void smoke(Oracle oracle) throws Exception {
        // Namespace
        var ns = Oracle.namespace("my-org");

        // relations
        var member = ns.relation("member");
        var flag = ns.relation("flag");

        // Group membersip
        var userMembers = ns.subject("Users", member);
        var adminMembers = ns.subject("Admins", member);
        var helpDeskMembers = ns.subject("HelpDesk", member);
        var managerMembers = ns.subject("Managers", member);
        var technicianMembers = ns.subject("Technicians", member);
        var abcTechMembers = ns.subject("ABCTechnicians", member);
        var flaggedTechnicianMembers = ns.subject(abcTechMembers.name(), flag);

        // Flagged subjects for testing
        var egin = ns.subject("Egin", flag);
        var ali = ns.subject("Ali", flag);
        var gl = ns.subject("G l", flag);
        var fuat = ns.subject("Fuat", flag);

        // Subjects
        var jale = ns.subject("Jale");
        var irmak = ns.subject("Irmak");
        var hakan = ns.subject("Hakan");
        var demet = ns.subject("Demet");
        var can = ns.subject("Can");
        var burcu = ns.subject("Burcu");

        // Map direct edges. Transitive edges added as a side effect
        CompletableFuture.allOf(oracle.map(helpDeskMembers, adminMembers), oracle.map(ali, adminMembers),
                                oracle.map(ali, userMembers), oracle.map(burcu, userMembers),
                                oracle.map(can, userMembers), oracle.map(managerMembers, userMembers),
                                oracle.map(technicianMembers, userMembers), oracle.map(demet, helpDeskMembers),
                                oracle.map(egin, helpDeskMembers), oracle.map(egin, userMembers),
                                oracle.map(fuat, managerMembers), oracle.map(gl, managerMembers),
                                oracle.map(hakan, technicianMembers), oracle.map(irmak, technicianMembers),
                                oracle.map(abcTechMembers, technicianMembers),
                                oracle.map(flaggedTechnicianMembers, technicianMembers),
                                oracle.map(jale, abcTechMembers))
                         .get();

        // Protected resource namespace
        var docNs = Oracle.namespace("Document");
        // Permission
        var view = docNs.relation("View");
        // Protected Object
        var object123View = docNs.object("123", view);

        // Users can View Document 123
        Assertion tuple = userMembers.assertion(object123View);
        oracle.add(tuple).get();

        // Direct subjects that can View the document
        var viewers = oracle.read(object123View);

        // Direct objects that can User member can view
        var viewable = oracle.read(userMembers);

        // Assert flagged technicians can directly view the document
        Assertion grantTechs = flaggedTechnicianMembers.assertion(object123View);
        oracle.add(grantTechs).get();

        // Now have 2 direct subjects that can view the doc
        viewers = oracle.read(object123View);

        // flagged has direct view
        viewable = oracle.read(flaggedTechnicianMembers);

        // Filter direct on flagged relation
        var flaggedViewers = oracle.read(flag, object123View);

        // Transitive subjects that can view the document
        var inferredViewers = oracle.expand(object123View);

        // Transitive subjects filtered by flag predicate
        var inferredFlaggedViewers = oracle.expand(flag, object123View);

        // Remove them
        oracle.remove(abcTechMembers, technicianMembers).get();

        // Remove our assertion
        oracle.delete(tuple).get();

        // Some deletes
        oracle.delete(abcTechMembers).get();
        oracle.delete(flaggedTechnicianMembers).get();
    }

    @CEntryPoint
    private static ObjectHandle createByteBuffer(IsolateThread renderingContext, Pointer address, int length) {
        ByteBuffer direct = CTypeConversion.asByteBuffer(address, length);
        ByteBuffer copy = ByteBuffer.allocate(length);
        copy.put(direct).rewind();
        return ObjectHandles.getGlobal().create(copy);
    }

    private static String launch(ByteBuffer paramBytes, char[] pwd) throws InvalidProtocolBufferException,
                                                                    GeneralSecurityException {
        try {
            return launch(DemesneParameters.parseFrom(paramBytes), pwd);
        } finally {
            Arrays.fill(pwd, ' ');
        }
    }

    @CEntryPoint
    private static ObjectHandle launch(@CEntryPoint.IsolateThreadContext IsolateThread domainContext,
                                       IsolateThread controlContext, ObjectHandle parameters,
                                       ObjectHandle ksPassword) throws GeneralSecurityException {
        ByteBuffer paramBytes = ObjectHandles.getGlobal().get(parameters);
        ObjectHandles.getGlobal().destroy(parameters);

        char[] pwd = ObjectHandles.getGlobal().get(ksPassword);
        ObjectHandles.getGlobal().destroy(ksPassword);

        String canonicalPath;
        try {
            canonicalPath = launch(paramBytes, pwd);
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot launch demesne", e);
            return ObjectHandles.getGlobal().create(' ');
        } finally {
            Arrays.fill(pwd, ' ');
        }
        return ObjectHandles.getGlobal().create(canonicalPath);
    }

    private final SubDomain                  domain;
    private final List<ProcessDomain>        domains = new ArrayList<>();
    private final Map<ProcessDomain, Router> routers = new HashMap<>();

    private final Stereotomy stereotomy;

    public Demesne() {
        domain = null;
        stereotomy = null;
    }

    public Demesne(DemesneParameters parameters, char[] pwd) throws GeneralSecurityException {
        final var kpa = parameters.getKeepAlive();
        Duration keepAlive = Duration.ofSeconds(kpa.getSeconds(), kpa.getNanos());
        DomainSocketAddress inbound = new DomainSocketAddress(Path.of(parameters.getCommDirectory())
                                                                  .resolve(getInbound())
                                                                  .toFile());
        DomainSocketAddress outbound = new DomainSocketAddress(Path.of(parameters.getCommDirectory())
                                                                   .resolve(parameters.getOutbound())
                                                                   .toFile());
        KERL kerl = null;

        final var password = Arrays.copyOf(pwd, pwd.length);
        stereotomy = new StereotomyImpl(new JksKeyStore(KeyStore.getInstance("JKS"), () -> password), kerl,
                                        SecureRandom.getInstanceStrong());

        @SuppressWarnings("unchecked")
        ControlledIdentifier<SelfAddressingIdentifier> identifier = (ControlledIdentifier<SelfAddressingIdentifier>) stereotomy.controlOf(Identifier.from(parameters.getMember()));
        ControlledIdentifierMember member = new ControlledIdentifierMember(identifier);
        Context<? extends Member> context = Context.newBuilder().build();

        var enclave = new Enclave(member, inbound, ForkJoinPool.commonPool(), outbound, keepAlive, ctxId -> {
            throw new UnsupportedOperationException("Not yet implemented");
        });
        var router = enclave.router(ForkJoinPool.commonPool());
        var params = Parameters.newBuilder();

        var runtime = RuntimeParameters.newBuilder()
                                       .setCommunications(router)
                                       .setExec(ForkJoinPool.commonPool())
                                       .setScheduler(Executors.newSingleThreadScheduledExecutor())
                                       .setKerl(() -> {
                                           try {
                                               return member.kerl().get();
                                           } catch (InterruptedException e) {
                                               Thread.currentThread().interrupt();
                                               return null;
                                           } catch (ExecutionException e) {
                                               throw new IllegalStateException(e.getCause());
                                           }
                                       })
                                       .setContext(context)
                                       .setFoundation(parameters.getFoundation());
        domain = new SubDomain(member, params, runtime,
                               new TransactionConfiguration(ForkJoinPool.commonPool(),
                                                            Executors.newSingleThreadScheduledExecutor()));
    }

    public void before() throws Exception {
        var scheduler = Executors.newScheduledThreadPool(2);
        var ffParams = com.salesforce.apollo.fireflies.Parameters.newBuilder();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var prefix = UUID.randomUUID().toString();
        Path checkpointDirBase = Path.of("target", "ct-chkpoints-" + Entropy.nextBitsStreamLong());
        Utils.clean(checkpointDirBase.toFile());
        var params = params();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(params.getDigestAlgorithm()), entropy);

        var identities = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(), controlled -> controlled));

        Digest group = DigestAlgorithm.DEFAULT.getOrigin();
        var foundation = Foundation.newBuilder();
        identities.keySet().forEach(d -> foundation.addMembership(d.toDigeste()));
        var sealed = FoundationSeal.newBuilder().setFoundation(foundation).build();
        TransactionConfiguration txnConfig = new TransactionConfiguration(Executors.newFixedThreadPool(2),
                                                                          Executors.newSingleThreadScheduledExecutor());
        identities.forEach((digest, id) -> {
            var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getLast(), CARDINALITY, 0.2, 3);
            final var member = new ControlledIdentifierMember(id);
            var localRouter = new LocalServer(prefix, member,
                                              Executors.newSingleThreadExecutor()).router(ServerConnectionCache.newBuilder().setTarget(30), ForkJoinPool.commonPool());
            var node = new ProcessDomain(group, member, params, "jdbc:h2:mem:", checkpointDirBase,
                                         RuntimeParameters.newBuilder()
                                                          .setFoundation(sealed)
                                                          .setScheduler(scheduler)
                                                          .setContext(context)
                                                          .setExec(ForkJoinPool.commonPool())
                                                          .setCommunications(localRouter),
                                         new InetSocketAddress(0), ffParams, txnConfig);
            domains.add(node);
            routers.put(node, localRouter);
            localRouter.start();
        });
    }

    public void smokin() throws Exception {
        long then = System.currentTimeMillis();
        final var gossipDuration = Duration.ofMillis(10);
        final var countdown = new CountDownLatch(domains.size());
        final var seeds = Collections.singletonList(new Seed(domains.get(0).getMember().getEvent().getCoordinates(),
                                                             new InetSocketAddress(0)));
        domains.forEach(d -> {
            d.getFoundation().register((context, viewId, joins, leaves) -> {
                if (context.totalCount() == CARDINALITY) {
                    System.out.println(String.format("Full view: %s members: %s on: %s", viewId, context.totalCount(),
                                                     d.getMember().getId()));
                    countdown.countDown();
                } else {
                    System.out.println(String.format("Members joining: %s members: %s on: %s", viewId,
                                                     context.totalCount(), d.getMember().getId()));
                }
            });
        });
        // start seed
        final var scheduler = Executors.newScheduledThreadPool(2);
        final var started = new AtomicReference<>(new CountDownLatch(1));

        domains.get(0)
               .getFoundation()
               .start(() -> started.get().countDown(), gossipDuration, Collections.emptyList(), scheduler);

        started.set(new CountDownLatch(CARDINALITY - 1));
        domains.subList(1, domains.size()).forEach(d -> {
            d.getFoundation().start(() -> started.get().countDown(), gossipDuration, seeds, scheduler);
        });
        System.out.println();
        System.out.println("******");
        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + domains.size() + " members");
        System.out.println("******");
        System.out.println();
        domains.forEach(n -> n.start());
        final var activated = Utils.waitForCondition(60_000, 1_000,
                                                     () -> domains.stream().filter(c -> !c.active()).count() == 0);

        if (!activated) {
            System.out.println("Domains did not become active : "
            + (domains.stream().filter(c -> !c.active()).toList()));
        }
        System.out.println();
        System.out.println("******");
        System.out.println("Domains have activated in " + (System.currentTimeMillis() - then) + " Ms across all "
        + domains.size() + " members");
        System.out.println("******");
        System.out.println();
        var oracle = domains.get(0).getDelphi();
        oracle.add(new Oracle.Namespace("test")).get();
        smoke(oracle);
    }

    private String getInbound() {
        // TODO Auto-generated method stub
        return null;
    }

    private Builder params() {
        var params = Parameters.newBuilder()
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setGossipDuration(Duration.ofMillis(50))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(50))
                                                              .setBatchInterval(Duration.ofMillis(100))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(3000)
                                                              .build())
                               .setCheckpointBlockDelta(200);

        params.getProducer().ethereal().setNumberOfEpochs(5);
        return params;
    }
}
