package com.salesforce.apollo.archipelago.server;

import com.macasaet.fernet.Key;
import com.macasaet.fernet.Token;
import com.salesforce.apollo.archipelago.Constants;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicReference;

import static com.salesforce.apollo.archipelago.server.FernetServerInterceptor.AUTH_HEADER_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ServerInterceptorTest {

    private final FernetServerInterceptor           target     = new FernetServerInterceptor();
    private final ServerCall<Object, Object>        serverCall = (ServerCall<Object, Object>) mock(ServerCall.class);
    private final ServerCallHandler<Object, Object> next       = (ServerCallHandler<Object, Object>) mock(
    ServerCallHandler.class);
    private       Token                             token;

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
    public void callNextStageWithContextKeyOnValidHeader() {
        Metadata metadata = new Metadata();
        metadata.put(Constants.AuthorizationMetadataKey, AUTH_HEADER_PREFIX + token.serialise());
        final AtomicReference<FernetServerInterceptor.HashedToken> actualToken = new AtomicReference<>();
        when(next.startCall(any(), any())).thenAnswer(i -> {
            actualToken.set(FernetServerInterceptor.AccessTokenContextKey.get());
            return null;
        });
        target.interceptCall(serverCall, metadata, next);
        verify(serverCall, never()).close(any(), any());
        verify(next).startCall(any(), any());
        assertEquals(token.serialise(), actualToken.get().token().serialise());
    }

    @Test
    public void closesCallOnInvalidHeader() {
        Metadata metadata = new Metadata();
        metadata.put(Constants.AuthorizationMetadataKey, "Bbb");
        target.interceptCall(serverCall, metadata, next);
        verify(serverCall).close(any(), any());
        verify(next, never()).startCall(any(), any());
    }

    @Test
    public void closesCallOnInvalidToken() {
        Metadata metadata = new Metadata();
        metadata.put(Constants.AuthorizationMetadataKey, "Bearer Invalid Token");
        target.interceptCall(serverCall, metadata, next);
        System.out.println("\n\n*** Exception above expected ***\n");
        verify(serverCall).close(any(), any());
        verify(next, never()).startCall(any(), any());
    }

    @Test
    public void closesCallOnMissingHeader() {
        target.interceptCall(serverCall, new Metadata(), next);
        verify(serverCall).close(any(), any());
        verify(next, never()).startCall(any(), any());
    }
}
