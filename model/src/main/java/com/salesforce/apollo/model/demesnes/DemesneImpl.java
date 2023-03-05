/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.model.proto.Request;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesforce.apollo.archipelago.Enclave;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.membership.stereotomy.IdentifierMember;
import com.salesforce.apollo.model.Domain.TransactionConfiguration;
import com.salesforce.apollo.model.SubDomain;
import com.salesforce.apollo.model.comms.SigningClient;
import com.salesforce.apollo.model.comms.SigningService;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.CommonKERLClient;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLAdapter;
import com.salesforce.apollo.thoth.Ani;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
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

    public class DemesneSigningMember extends DemesneMember implements SigningMember {

        public DemesneSigningMember(EstablishmentEvent event) {
            super(event);
        }

        @Override
        public SignatureAlgorithm algorithm() {
            return event.getAuthentication().getAlgorithm();
        }

        @Override
        public JohnHancock sign(ByteString... message) {
            final var builder = Request.newBuilder().setCoordinates(event.getCoordinates().toEventCoords());
            for (var m : message) {
                builder.addContent(m);
            }
            return JohnHancock.from(signer.sign(builder.build()));
        }

        @Override
        public JohnHancock sign(InputStream message) {
            try {
                return JohnHancock.from(signer.sign(Request.newBuilder()
                                                           .setCoordinates(event.getCoordinates().toEventCoords())
                                                           .addContent(ByteString.readFrom(message))
                                                           .build()));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot sign message", e);
            }
        }
    }

    private static final Class<? extends Channel> channelType    = getChannelType();
    private static final EventLoopGroup           eventLoopGroup = getEventLoopGroup();
    private static final Logger                   log            = LoggerFactory.getLogger(DemesneImpl.class);

    private static ClientInterceptor clientInterceptor(Digest ctx) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, io.grpc.Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Router.METADATA_CONTEXT_KEY, qb64(ctx));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    private final SubDomain     domain;
    private final String        inbound;
    private final KERL          kerl;
    private SigningService      signer;
    private final AtomicBoolean started = new AtomicBoolean();
    private final Stereotomy    stereotomy;
    private EventValidation     validation;

    public DemesneImpl(DemesneParameters parameters, char[] pwd) throws GeneralSecurityException, IOException {
        final var kpa = parameters.getKeepAlive();
        Duration keepAlive = !kpa.isInitialized() ? Duration.ofMillis(1)
                                                  : Duration.ofSeconds(kpa.getSeconds(), kpa.getNanos());
        final var commDirectory = Path.of(parameters.getCommDirectory().isEmpty() ? System.getProperty("user.home")
                                                                                  : parameters.getCommDirectory());
        final var address = commDirectory.resolve(UUID.randomUUID().toString()).toFile();
        inbound = address.getCanonicalPath();

        final var password = Arrays.copyOf(pwd, pwd.length);
        Arrays.fill(pwd, ' ');

        final var keystore = KeyStore.getInstance("JKS");
        keystore.load(parameters.getKeyStore().newInput(), password);
        Digest kerlContext = Digest.from(parameters.getKerlContext());
        kerl = kerlFrom(parameters, commDirectory, kerlContext);
        stereotomy = new StereotomyImpl(new JksKeyStore(keystore, () -> password), kerl,
                                        SecureRandom.getInstanceStrong());

        ControlledIdentifierMember member;
        try {
            member = new ControlledIdentifierMember(stereotomy.controlOf((SelfAddressingIdentifier) Identifier.from(parameters.getMember()))
                                                              .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            domain = null;
            return;
        } catch (ExecutionException e) {
            throw new IllegalStateException("Invalid state", e.getCause());
        }

        var context = Context.newBuilder().build();
        context.activate(member);
        var exec = Executors.newVirtualThreadPerTaskExecutor();

        domain = subdomainFrom(parameters, keepAlive, commDirectory, address, member, context, exec);
        signer = signerFrom(parameters, commDirectory);
        Duration timeout = Duration.ofSeconds(parameters.getTimeout().getSeconds(), parameters.getTimeout().getNanos());
        validation = new Ani(kerlContext, kerl).eventValidation(timeout);
    }

    @Override
    public boolean active() {
        return domain == null ? false : domain.active();
    }

    public String getInbound() {
        return inbound;
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        domain.start();
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

    private CachingKERL kerlFrom(DemesneParameters parameters, final Path commDirectory, Digest kerlContext) {
        final var kerlService = parameters.getKerlService();
        final var file = commDirectory.resolve(kerlService).toFile();
        final var serverAddress = new DomainSocketAddress(file);
        log.error("Kerl service: {}\n comm directory: {}\n context: {}\n file: {}\n address: {}", kerlService,
                  commDirectory, kerlContext, file, serverAddress);
        NettyChannelBuilder.forAddress(serverAddress);
        return new CachingKERL(f -> {
            var channel = NettyChannelBuilder.forAddress(serverAddress)
                                             .intercept(clientInterceptor(kerlContext))
                                             .eventLoopGroup(eventLoopGroup)
                                             .channelType(channelType)
                                             .keepAliveTime(1, TimeUnit.SECONDS)
                                             .usePlaintext()
                                             .build();
            var stub = KERLServiceGrpc.newFutureStub(channel);
            return f.apply(new KERLAdapter(new CommonKERLClient(stub, null), DigestAlgorithm.DEFAULT));
        });
    }

    private void registerContext(Digest ctxId) {
        // TODO Auto-generated method stub
    }

    private SigningClient signerFrom(DemesneParameters parameters, final Path commDirectory) {
        return new SigningClient(NettyChannelBuilder.forAddress(new DomainSocketAddress(commDirectory.resolve(parameters.getSigningService())
                                                                                                     .toFile()))
                                                    .eventLoopGroup(eventLoopGroup)
                                                    .channelType(channelType)
                                                    .keepAliveTime(1, TimeUnit.SECONDS)
                                                    .usePlaintext()
                                                    .build(),
                                 null);
    }

    private SubDomain subdomainFrom(DemesneParameters parameters, Duration keepAlive, final Path commDirectory,
                                    final File address, ControlledIdentifierMember member, Context<Member> context,
                                    ExecutorService exec) {
        return new SubDomain(member, Parameters.newBuilder(),
                             RuntimeParameters.newBuilder()
                                              .setCommunications(new Enclave(member, new DomainSocketAddress(address),
                                                                             exec,
                                                                             new DomainSocketAddress(commDirectory.resolve(parameters.getOutbound())
                                                                                                                  .toFile()),
                                                                             keepAlive, ctxId -> {
                                                                                 registerContext(ctxId);
                                                                             }).router(exec))
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
