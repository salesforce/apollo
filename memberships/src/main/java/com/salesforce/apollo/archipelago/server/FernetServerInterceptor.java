package com.salesforce.apollo.archipelago.server;

import com.macasaet.fernet.Token;
import com.salesforce.apollo.archipelago.Constants;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class FernetServerInterceptor implements ServerInterceptor {
    public static final  String                   AUTH_HEADER_PREFIX    = "Bearer ";
    public static final  Context.Key<HashedToken> AccessTokenContextKey = Context.key("AccessToken");
    private static final Logger                   LOGGER                = LoggerFactory.getLogger(
    FernetServerInterceptor.class);
    private final        DigestAlgorithm          algorithm;

    public FernetServerInterceptor() {
        this(DigestAlgorithm.DEFAULT);
    }

    public FernetServerInterceptor(DigestAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        String authHeader = headers.get(Constants.AuthorizationMetadataKey);
        if (authHeader == null) {
            String msg = Constants.AuthorizationMetadataKey.name() + " header not found";
            LOGGER.warn(msg);
            call.close(Status.UNAUTHENTICATED.withDescription(msg), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }
        if (!authHeader.startsWith(AUTH_HEADER_PREFIX)) {
            String msg =
            Constants.AuthorizationMetadataKey.name() + " header does not start with " + AUTH_HEADER_PREFIX;
            LOGGER.warn(msg);
            call.close(Status.UNAUTHENTICATED.withDescription(msg), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }
        DelayedServerCallListener<ReqT> delayedListener = new DelayedServerCallListener<>();
        Context context = Context.current(); // we must call this on the right thread
        try {
            var serialized = authHeader.substring(AUTH_HEADER_PREFIX.length());

            deserialize(serialized).whenComplete((token, e) -> context.run(() -> {
                if (e == null) {
                    delayedListener.setDelegate(
                    Contexts.interceptCall(Context.current().withValue(AccessTokenContextKey, token), call, headers,
                                           next));
                } else {
                    delayedListener.setDelegate(handleException(e, call));
                }
            }));
        } catch (Exception e) {
            return handleException(e, call);
        }
        return delayedListener;
    }

    private CompletionStage<HashedToken> deserialize(String serialized) {
        if (serialized.equals("Invalid Token")) {
            return CompletableFuture.failedFuture(new RuntimeException("invalid token"));
        }
        try {
            return CompletableFuture.completedFuture(
            new HashedToken(algorithm.digest(serialized), Token.fromString(serialized)));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> handleException(Throwable e, ServerCall<ReqT, RespT> call) {
        String msg = Constants.AuthorizationMetadataKey.name() + " header validation failed: " + e.getMessage();
        LOGGER.warn(msg, e);
        call.close(Status.UNAUTHENTICATED.withDescription(msg).withCause(e), new Metadata());
        return new ServerCall.Listener<ReqT>() {
        };
    }

    /**
     * This record provides the hash of the Token.serialized() string using the interceptor's DigestAlgorithm
     *
     * @param hash  - the hash of the serialized token String
     * @param token - the deserialized Token
     */
    public record HashedToken(Digest hash, Token token) {
        @Override
        public boolean equals(Object o) {
            if (o instanceof HashedToken ht) {
                return hash.equals(ht.hash);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return hash.hashCode();
        }
    }
}
