/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.domain;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.mem.MemKERL;

/**
 * @author hal.hildebrand
 *
 */
public class DemesneTest {

//    @Test
    public void smokin() throws Exception {
        var commDirectory = Path.of("target").resolve(UUID.randomUUID().toString());
        Files.createDirectories(commDirectory);
        final var ksPassword = new char[] { 'f', 'o', 'o' };
        final var ks = KeyStore.getInstance("JKS");
        ks.load(null, ksPassword);
        final var keystore = new JksKeyStore(ks, () -> ksPassword);
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        Stereotomy controller = new StereotomyImpl(keystore, kerl, SecureRandom.getInstanceStrong());
        var identifier = controller.newIdentifier().get();
        var baos = new ByteArrayOutputStream();
        ks.store(baos, ksPassword);

        var parameters = DemesneParameters.newBuilder()
                                          .setMember(identifier.getIdentifier().toIdent())
                                          .setKeyStore(ByteString.copyFrom(baos.toByteArray()))
                                          .setCommDirectory(commDirectory.toString())
                                          .build();
        var demesne = new Demesne(parameters, ksPassword);
        assertNotNull(demesne);
    }
}
