/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionFactory;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class LocalRouter extends Router {

    public static class LocalServerConnectionFactory implements ServerConnectionFactory {

        @Override
        public ManagedChannel connectTo(Member to, SigningMember from) {
            ClientInterceptor clientInterceptor = new ClientInterceptor() {

                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                           CallOptions callOptions, Channel next) {
                    ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                    return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                        @Override
                        public void start(Listener<RespT> responseListener, Metadata headers) {
                            headers.put(AUTHORIZATION_METADATA_KEY, qb64(from.getId()));
                            super.start(responseListener, headers);
                        }
                    };
                }
            };
            return InProcessChannelBuilder.forName(qb64(to.getId()))
                                          .directExecutor()
                                          .intercept(clientInterceptor)
                                          .build();
        }
    }

    private static final class ThreadIdentity implements ClientIdentity {
        @Override
        public X509Certificate getCert() {
            Member member = CLIENT_ID_CONTEXT_KEY.get();
            if (member == null) {
                return null;
            }
            X509Certificate x509Certificate = member.getCertificate();
            return x509Certificate;
        }

        @Override
        public Certificate[] getCerts() {
            return new Certificate[] { getCert() };
        }

        @Override
        public Digest getFrom() {
            Member member = CLIENT_ID_CONTEXT_KEY.get();
            return member == null ? null : member.getId();
        }

    }

    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization",
                                                                                          Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<Member>  CLIENT_ID_CONTEXT_KEY      = Context.key("from.id");
    public static final ThreadIdentity       LOCAL_IDENTITY             = new ThreadIdentity();
    private static final Logger              log                        = LoggerFactory.getLogger(LocalRouter.class);
    private static final Map<Digest, Member> serverMembers              = new ConcurrentHashMap<>();

    private final Member member;
    private final Server server;

    public LocalRouter(Member member, ServerConnectionCache.Builder builder, Executor executor) {
        this(member, builder, new MutableHandlerRegistry(), executor);
    }

    public LocalRouter(Member member, ServerConnectionCache.Builder builder, MutableHandlerRegistry registry,
            Executor executor) {
        super(builder.setFactory(new LocalServerConnectionFactory()).build(), registry);
        this.member = member;
        serverMembers.put(member.getId(), member);

        server = InProcessServerBuilder.forName(qb64(member.getId()))
                                       .executor(executor)
                                       .intercept(new ServerInterceptor() {

                                           @Override
                                           public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                                                        final Metadata requestHeaders,
                                                                                                        ServerCallHandler<ReqT, RespT> next) {
                                               String id = requestHeaders.get(AUTHORIZATION_METADATA_KEY);
                                               if (id == null) {
                                                   log.error("No member id in call headers: {}", requestHeaders.keys());
                                                   throw new IllegalStateException("No member ID in call");
                                               }
                                               Member member = serverMembers.get(digest(id));
                                               if (member == null) {
                                                   call.close(Status.INTERNAL.withCause(new NullPointerException(
                                                           "Member is null"))
                                                                             .withDescription("Member is null for id: "
                                                                                     + id),
                                                              null);
                                                   return new ServerCall.Listener<ReqT>() {
                                                   };
                                               }
                                               Context ctx = Context.current().withValue(CLIENT_ID_CONTEXT_KEY, member);
                                               return Contexts.interceptCall(ctx, call, requestHeaders, next);
                                           }

                                       })
                                       .fallbackHandlerRegistry(registry)
                                       .build();
    }

    @Override
    public void close() {
        server.shutdownNow();
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unknown server state as we've been interrupted in the process of shutdown",
                    e);
        }
        super.close();
        serverMembers.remove(member.getId());
    }

    @Override
    public ClientIdentity getClientIdentityProvider() {
        return LOCAL_IDENTITY;
    }

    @Override
    public void start() {
        try {
            serverMembers.put(member.getId(), member);
            server.start();
        } catch (IOException e) {
            log.error("Cannot start in process server for: " + member, e);
        }
        log.info("Starting server for: " + member);
    }

}
