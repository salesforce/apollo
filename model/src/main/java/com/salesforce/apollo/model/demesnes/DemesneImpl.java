/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes;

import static com.salesforce.apollo.archipelago.Router.clientInterceptor;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.demesne.proto.SubContext;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesforce.apollo.archipelago.Enclave;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.membership.stereotomy.IdentifierMember;
import com.salesforce.apollo.model.Domain.TransactionConfiguration;
import com.salesforce.apollo.model.SubDomain;
import com.salesforce.apollo.model.demesnes.comm.OuterContextClient;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.CommonKERLClient;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLAdapter;
import com.salesforce.apollo.thoth.Ani;
import com.salesforce.apollo.thoth.Thoth;
import com.salesforce.apollo.utils.Hex;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * Isolate for the Apollo SubDomain stack
 *
 * @author hal.hildebrand
 *
 */
public class DemesneImpl implements Demesne {
    public class DemesneMember implements Member {
        protected EstablishmentEvent event;
        private final Digest         id;

        public DemesneMember(EstablishmentEvent event) {
            this.event = event;
            if (event.getIdentifier() instanceof SelfAddressingIdentifier sai) {
                id = sai.getDigest();
            } else {
                throw new IllegalArgumentException("Only self addressing identifiers supported: "
                + event.getIdentifier());
            }
        }

        @Override
        public int compareTo(Member m) {
            return id.compareTo(m.getId());
        }

        @Override
        public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return validation.filtered(event.getCoordinates(), threshold, signature, message);
        }

        @Override
        public Digest getId() {
            return id;
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return validation.verify(event.getCoordinates(), signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return validation.verify(event.getCoordinates(), threshold, signature, message);
        }
    }

    private static final Class<? extends Channel> channelType    = getChannelType();
    private static final EventLoopGroup           eventLoopGroup = getEventLoopGroup();
    private static final Logger                   log            = LoggerFactory.getLogger(DemesneImpl.class);

    private Context<Member>          context;
    private SubDomain                domain;
    private Enclave                  enclave;
    private final ExecutorService    exec;
    private final KERL               kerl;
    private final OuterContextClient outer;
    private final DemesneParameters  parameters;
    private final AtomicBoolean      started = new AtomicBoolean();
    private final Stereotomy         stereotomy;
    private final Thoth              thoth;
    private final EventValidation    validation;

    public DemesneImpl(DemesneParameters parameters) throws GeneralSecurityException, IOException {
        assert parameters.hasContext() : "Must define context id";
        this.parameters = parameters;
        exec = Executors.newVirtualThreadPerTaskExecutor();
        context = Context.newBuilder().setId(Digest.from(parameters.getContext())).build();
        final var commDirectory = commDirectory();
        var outerContextAddress = commDirectory.resolve(parameters.getParent()).toFile();

        outer = outerFrom(outerContextAddress);
        final var pwd = new byte[64];
        final var entropy = SecureRandom.getInstanceStrong();
        entropy.nextBytes(pwd);
        final var password = Hex.hexChars(pwd);
        final Supplier<char[]> passwordProvider = () -> password;
        final var keystore = KeyStore.getInstance("JKS");

        keystore.load(null, password);

        kerl = kerlFrom(outerContextAddress);
        validation = new Ani(context.getId(), kerl).eventValidation(Duration.ofSeconds(
                                                                                       parameters.getTimeout()
                                                                                                 .getSeconds(),
                                                                                       parameters.getTimeout()
                                                                                                 .getNanos()));
        stereotomy = new StereotomyImpl(new JksKeyStore(keystore, passwordProvider), kerl, entropy);

        thoth = new Thoth(stereotomy);
    }

    @Override
    public boolean active() {
        return domain == null ? false : domain.active();
    }

    @Override
    public void commit(EventCoords coordinates) {
        thoth.commit(EventCoordinates.from(coordinates));
        final var commDirectory = commDirectory();
        var outerContextAddress = commDirectory.resolve(parameters.getParent()).toFile();

        context.activate(thoth.member());

        log.info("Creating Demesne: {} bridge: {} on: {}", context.getId(), outerContextAddress,
                 thoth.member().getId());
        final var kpa = parameters.getKeepAlive();
        Duration keepAlive = !kpa.isInitialized() ? Duration.ofMillis(1)
                                                  : Duration.ofSeconds(kpa.getSeconds(), kpa.getNanos());

        enclave = new Enclave(thoth.member(), new DomainSocketAddress(outerContextAddress), exec,
                              new DomainSocketAddress(commDirectory.resolve(parameters.getPortal()).toFile()),
                              keepAlive, ctxId -> registerContext(ctxId));
        domain = subdomainFrom(parameters, keepAlive, commDirectory, outerContextAddress, thoth.member(), context,
                               exec);
    }

    @Override
    public DelegatedInceptionEvent inception(Ident id, Builder<SelfAddressingIdentifier> specification) {
        var identifier = (SelfAddressingIdentifier) Identifier.from(id);
        return thoth.inception(identifier, specification);
    }

    @Override
    public DelegatedRotationEvent rotate(RotationSpecification.Builder specification) {
        try {
            return thoth.rotate(specification).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            log.error("Unable to rotate member: {}", thoth.member().getId(), e.getCause());
            return null;
        }
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        if (domain == null) {
            log.error("Inception has not occurred");
        } else {
            domain.start();
        }
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        domain.stop();
    }

    @Override
    public void viewChange(Digest viewId, List<EventCoordinates> joining, List<Digest> leaving) {
        joining.forEach(coords -> {
            EstablishmentEvent keyEvent;
            try {
                keyEvent = kerl.getKeyState(coords).thenApply(ke -> (EstablishmentEvent) ke).get();
                domain.activate(new IdentifierMember(keyEvent));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Error retrieving last establishment event for: {}", coords, e.getCause());
            }
        });
        leaving.forEach(id -> domain.getContext().remove(id));
    }

    private Path commDirectory() {
        return Path.of(parameters.getCommDirectory().isEmpty() ? System.getProperty("user.home")
                                                               : parameters.getCommDirectory());
    }

    private CachingKERL kerlFrom(File address) {
        Digest kerlContext = context.getId();
        final var serverAddress = new DomainSocketAddress(address);
        log.info("Kerl context: {} address: {}", kerlContext, serverAddress);
        NettyChannelBuilder.forAddress(serverAddress);
        return new CachingKERL(f -> {
            ManagedChannel channel = null;
            try {
                channel = NettyChannelBuilder.forAddress(serverAddress)
                                             .intercept(clientInterceptor(kerlContext))
                                             .eventLoopGroup(eventLoopGroup)
                                             .channelType(channelType)
                                             .keepAliveTime(1, TimeUnit.SECONDS)
                                             .usePlaintext()
                                             .build();
                var stub = KERLServiceGrpc.newFutureStub(channel);
                return f.apply(new KERLAdapter(new CommonKERLClient(stub, null), DigestAlgorithm.DEFAULT));
            } catch (Throwable t) {
                return f.apply(null);
            } finally {
                if (channel != null) {
                    channel.shutdown();
                }
            }
        });
    }

    private OuterContextClient outerFrom(File address) {
        return new OuterContextClient(NettyChannelBuilder.forAddress(new DomainSocketAddress(address))
                                                         .intercept(clientInterceptor(context.getId()))
                                                         .eventLoopGroup(eventLoopGroup)
                                                         .channelType(channelType)
                                                         .keepAliveTime(1, TimeUnit.SECONDS)
                                                         .usePlaintext()
                                                         .build(),
                                      null);
    }

    private void registerContext(Digest ctxId) {
        outer.register(SubContext.newBuilder()
                                 .setEnclave(context.getId().toDigeste())
                                 .setContext(ctxId.toDigeste())
                                 .build());
    }

    private SubDomain subdomainFrom(DemesneParameters parameters, Duration keepAlive, final Path commDirectory,
                                    final File address, ControlledIdentifierMember member, Context<Member> context,
                                    ExecutorService exec) {
        return new SubDomain(member, Parameters.newBuilder(),
                             RuntimeParameters.newBuilder()
                                              .setCommunications(enclave.router(exec))
                                              .setExec(exec)
                                              .setScheduler(Executors.newScheduledThreadPool(5,
                                                                                             Thread.ofVirtual()
                                                                                                   .factory()))
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
                                              .setFoundation(parameters.getFoundation()),
                             new TransactionConfiguration(exec, Executors.newScheduledThreadPool(5, Thread.ofVirtual()
                                                                                                          .factory())));
    }
}
