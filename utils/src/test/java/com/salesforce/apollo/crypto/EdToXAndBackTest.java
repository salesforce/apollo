/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.X25519PrivateKeyParameters;
import org.bouncycastle.util.Arrays;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class EdToXAndBackTest {
    @Test
    public void smokin() {
        var sigPair = SignatureAlgorithm.ED_25519.generateKeyPair();
        var encryptionPair = SignatureAlgorithm.ED_25519.toEncryption(sigPair);
        assertNotNull(encryptionPair);
    }

    @Test
    public void testEd25519ToX25519() {
        checkEd25519ToX25519Vector("be5bf46a933c8703fa48d0c4075c8fe35fb5f2358778c62008d7265ea6eb0858",
                                   "188dedb57fb265624e370e214eba35799cd17897f1d44663530606a2ed5cb57f",
                                   "2038f67c3fcfc38429819229d4c874d1f22540ab1349949a766cca0846363f28",
                                   "Ed25519 to X25519 vector #1");
        checkEd25519ToX25519Vector("7013fabacfa4bd6eafb75e9d2d426a1f956ccd9acb19b615d3041d3e0b3000e6",
                                   "c0418dcd2fc1da92d6fb07c2ae4e0e4ddd71819533326047deab1c8c882e806f",
                                   "ec3e66a867e1f383dbcda7084569ffced6af071e85cb20791523347c59ec3459",
                                   "Ed25519 to X25519 vector #2");
        // This vector checks proper normalization of 'u' (X25519 public key) when
        // converting from Ed25519 public key
        checkEd25519ToX25519Vector("f81fe2c27e3884dfa6c3a288f37d0ff5699ddade04b6c7dbc379c68a7e8129a0",
                                   "e0ee579cf0e094f9aa2c2f87caf8a2e48843fca000325b45400189991c684564",
                                   "4d8f5ab537e51507965ed841c35cb896ef6c474f789188cd3dd86dfb769ac661",
                                   "Ed25519 to X25519 vector #3");
    }

    private void checkEd25519ToX25519Vector(String ed25519SK, String x25519SK, String x25519PK, String text) {
        byte[] esk = Hex.decode(ed25519SK);
        byte[] xsk = Hex.decode(x25519SK);
        byte[] xpk = Hex.decode(x25519PK);

        // Check Ed25519 secret key converts to expected X25519 secret key
        {
            byte[] converted = Ed25519.toX25519PrivateKey(esk);
            assertTrue(Arrays.areEqual(xsk, converted), text);
        }

        // Derive X25519 public key from X25519 secret key and check
        {
            X25519PrivateKeyParameters x25519PrivateKeyParams = new X25519PrivateKeyParameters(xsk, 0);
            byte[] derived = x25519PrivateKeyParams.generatePublicKey().getEncoded();
            assertTrue(Arrays.areEqual(xpk, derived), text);
        }

        // Derive Ed25519 public key from Ed25519 secret key,
        // then convert Ed25519 public key to X25519 public key and check
        {
            Ed25519PrivateKeyParameters ed25519PrivateKeyParams = new Ed25519PrivateKeyParameters(esk, 0);
            byte[] derived = ed25519PrivateKeyParams.generatePublicKey().getEncoded();

            byte[] converted = Ed25519.toX25519PublicKey(derived);
            assertTrue(Arrays.areEqual(xpk, converted), text);
        }
    }
}
