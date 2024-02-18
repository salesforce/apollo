package com.salesforce.apollo.archipelago.server;

import com.macasaet.fernet.Token;
import com.salesforce.apollo.archipelago.Constants;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class FernetServerInterceptor implements ServerInterceptor {
    public static final  String             AUTH_HEADER_PREFIX    = "Bearer ";
    public static final  Context.Key<Token> AccessTokenContextKey = Context.key("AccessToken");
    private static final Logger             LOGGER                = LoggerFactory.getLogger(
    FernetServerInterceptor.class);

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

    private CompletionStage<Token> deserialize(String serialized) {
        if (serialized.equals("Invalid Token")) {
            CompletableFuture<Token> res = new CompletableFuture<>();
            res.completeExceptionally(new RuntimeException("invalid token"));
            return res;
        }
        try {
            return CompletableFuture.completedFuture(Token.fromString(serialized));
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
}
