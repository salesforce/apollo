package com.salesforce.apollo.archipelago.client;

import com.macasaet.fernet.Key;
import com.macasaet.fernet.Token;
import com.salesforce.apollo.archipelago.Constants;
import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static com.salesforce.apollo.archipelago.server.FernetServerInterceptor.AUTH_HEADER_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class FernetCallCredentialsTest {

    AtomicReference<Metadata>             actualMetadata = new AtomicReference<>();
    FernetCallCredentials.MetadataApplier applier        = new FernetCallCredentials.MetadataApplier() {
        @Override
        public void apply(Metadata headers) {
            actualMetadata.set(headers);
        }

        @Override
        public void fail(Status status) {
        }
    };
    ExecutorService                       executor       = Executors.newSingleThreadExecutor();

    private Token token;

    @Test
    public void asynchronous() throws InterruptedException {
        var target = FernetCallCredentials.asynchronous(() -> CompletableFuture.completedFuture(token));
        target.applyRequestMetadata(mock(FernetCallCredentials.RequestInfo.class), executor, applier);
        Thread.sleep(1000);
        assertEquals("%s%s".formatted(AUTH_HEADER_PREFIX, token.serialise()),
                     actualMetadata.get().get(Constants.AuthorizationMetadataKey));
    }

    @BeforeEach
    public void before() {
        final SecureRandom deterministicRandom = new SecureRandom() {
            private static final long serialVersionUID = 3075400891983079965L;

            public void nextBytes(final byte[] bytes) {
                for (int i = bytes.length; --i >= 0; bytes[i] = 1)
                    ;
            }
        };
        final Key key = Key.generateKey(deterministicRandom);

        token = Token.generate(deterministicRandom, key, "Hello, world!");
    }

    @Test
    public void blocking() throws InterruptedException {
        var target = FernetCallCredentials.blocking(() -> token);
        target.applyRequestMetadata(mock(FernetCallCredentials.RequestInfo.class), executor, applier);
        Thread.sleep(1000);
        assertEquals("%s%s".formatted(AUTH_HEADER_PREFIX, token.serialise()),
                     actualMetadata.get().get(Constants.AuthorizationMetadataKey));
    }

    @Test
    public void synchronous() {
        var target = FernetCallCredentials.synchronous(() -> token);
        target.applyRequestMetadata(mock(FernetCallCredentials.RequestInfo.class), Executors.newSingleThreadExecutor(),
                                    applier);
        assertEquals("%s%s".formatted(AUTH_HEADER_PREFIX, token.serialise()),
                     actualMetadata.get().get(Constants.AuthorizationMetadataKey));
    }
}
