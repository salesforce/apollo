/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.pal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.pal.proto.Decrypted;
import com.salesfoce.apollo.pal.proto.Encrypted;
import com.salesfoce.apollo.pal.proto.Secret;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesforce.apollo.pal.daemon.PalDaemon;

import io.netty.channel.unix.PeerCredentials;

/**
 * @author hal.hildebrand
 *
 */
public class PalClientTest {

    @Test
    public void smokin() throws Exception {
        Path socketPath = Path.of("target").resolve("smokin.socket");
        Files.deleteIfExists(socketPath);

        final var testLabel = "TEST_LABEL";
        Function<PeerCredentials, CompletableFuture<Set<String>>> retriever = principal -> {
            var fs = new CompletableFuture<Set<String>>();
            fs.complete(Set.of(testLabel));
            return fs;
        };
        var decrypters = new HashMap<String, Function<Encrypted, CompletableFuture<Decrypted>>>();

        decrypters.put("nb", e -> {
            var fs = new CompletableFuture<Decrypted>();
            fs.complete(Decrypted.newBuilder()
                                 .putSecrets("foo",
                                             Any.pack(ByteMessage.newBuilder()
                                                                 .setContents(ByteString.copyFromUtf8("bar"))
                                                                 .build()))
                                 .build());
            return fs;
        });

        var server = new PalDaemon(socketPath, retriever, decrypters);
        server.start();

        var palClient = new PalClient(socketPath);

        var secrets = new HashMap<String, Secret>();
        secrets.put("test",
                    Secret.newBuilder()
                          .addLabels(testLabel)
                          .setDecryptor("nb")
                          .setEncrypted(Any.pack(ByteMessage.newBuilder().build()))
                          .build());
        var result = palClient.decrypt(Encrypted.newBuilder().putAllSecrets(secrets).build());
        assertNotNull(result);
        assertEquals("bar",
                     result.getSecretsMap()
                           .get("foo")
                           .unpack(ByteMessage.class)
                           .getContents()
                           .toString(Charset.defaultCharset()));
    }
}
