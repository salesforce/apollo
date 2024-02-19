package com.salesforce.apollo.archipelago.client;

import com.macasaet.fernet.Token;
import com.salesforce.apollo.archipelago.Constants;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static com.salesforce.apollo.archipelago.server.FernetServerInterceptor.AUTH_HEADER_PREFIX;

public abstract class FernetCallCredentials extends CallCredentials {
    private static final Logger LOGGER = LoggerFactory.getLogger(FernetCallCredentials.class);

    public static FernetCallCredentials synchronous(SynchronousTokenProvider tokenProvider) {
        return new Synchronous(tokenProvider);
    }

    public static FernetCallCredentials blocking(BlockingTokenProvider tokenProvider) {
        return new Blocking(tokenProvider);
    }

    public static FernetCallCredentials asynchronous(AsyncTokenProvider tokenProvider) {
        return new Asynchronous(tokenProvider);
    }

    @Override
    public void thisUsesUnstableApi() {
    }

    protected void applyFailure(MetadataApplier applier, Throwable e) {
        String msg = "An exception when obtaining bearer token";
        LOGGER.error(msg, e);
        applier.fail(Status.UNAUTHENTICATED.withDescription(msg).withCause(e));
    }

    protected void applyToken(MetadataApplier applier, Token token) {
        if (token == null) {
            return;
        }
        Metadata metadata = new Metadata();
        metadata.put(Constants.AuthorizationMetadataKey, AUTH_HEADER_PREFIX + token.serialise());
        applier.apply(metadata);
    }

    public static class Synchronous extends FernetCallCredentials {

        private final SynchronousTokenProvider tokenProvider;

        public Synchronous(SynchronousTokenProvider tokenProvider) {
            this.tokenProvider = tokenProvider;
        }

        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
            try {
                applyToken(applier, tokenProvider.get());
            } catch (RuntimeException e) {
                applyFailure(applier, e);
            }
        }
    }

    public static class Blocking extends FernetCallCredentials {

        private final BlockingTokenProvider tokenProvider;

        public Blocking(BlockingTokenProvider tokenProvider) {
            this.tokenProvider = tokenProvider;
        }

        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
            appExecutor.execute(() -> {
                try {
                    applyToken(applier, tokenProvider.get());
                } catch (RuntimeException e) {
                    applyFailure(applier, e);
                }
            });
        }
    }

    public static class Asynchronous extends FernetCallCredentials {

        private final AsyncTokenProvider tokenProvider;

        public Asynchronous(AsyncTokenProvider jwtTokenProvider) {
            this.tokenProvider = jwtTokenProvider;
        }

        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
            tokenProvider.get().whenComplete((token, e) -> {
                if (token != null)
                    applyToken(applier, token);
                else
                    applyFailure(applier, e);
            });
        }
    }
}
