/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.salesforce.apollo.sterotomy.crypto.Blake3;

/**
 * @author hal.hildebrand
 *
 */
public class Blake3Tests {
    /**
     * 
     */
    private static final String LICENSE            = "src/test/resources/blake3.license";
    private static final byte[] testBytes          = "This is a string".getBytes(StandardCharsets.UTF_8);
    private static final byte[] testKeyedHashBytes = new byte[] { 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
                                                                  0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
                                                                  0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41,
                                                                  0x41, 0x41, 0x41, 0x41, 0x41 };

    private static final byte[] testVectorData = new byte[251];
    static {
        List<Byte> tmpList = new ArrayList<>();
        IntStream.range(0, 251).forEach(x -> tmpList.add((byte) x));
        for (int i = 0; i < testVectorData.length; i++) {
            testVectorData[i] = tmpList.get(i);
        }
    }

    @Test
    public void basicHash() {
        Blake3 hasher = Blake3.newInstance();
        hasher.update(testBytes);
        assertEquals("718b749f12a61257438b2ea6643555fd995001c9d9ff84764f93f82610a780f2", hasher.hexdigest());
    }

    public byte[] getTestVectorInput(int inputLen) {
        byte[] remainder = Arrays.copyOfRange(testVectorData, 0, inputLen % 251);
        byte[] input = new byte[inputLen];

        int x = 0;
        while (x + 251 < inputLen) {
            System.arraycopy(testVectorData, 0, input, x, 251);
            x += 251;
        }
        System.arraycopy(remainder, 0, input, inputLen - remainder.length, remainder.length);
        return input;
    }

    @Test
    public void officialTestVectors() {
        // Test vectors from the BLAKE3 repo:
        // https://github.com/BLAKE3-team/BLAKE3/tree/master/test_vectors
        // Comment from the json file:
        // Each test is an input length and three outputs, one for each of the hash,
        // keyed_hash, and derive_key modes.
        // The input in each case is filled with a 251-byte-long repeating pattern: 0,
        // 1, 2, ..., 249, 250, 0, 1, ...
        // The key used with keyed_hash is the 32-byte ASCII string given in the 'key'
        // field below.
        // For derive_key, the test input is used as the input key, and the context
        // string is 'BLAKE3 2019-12-27 16:29:52 test vectors context'.
        // (As good practice for following the security requirements of derive_key, test
        // runners should make that context string a hardcoded constant,
        // and we do not provided it in machine-readable form.)
        // Outputs are encoded as hexadecimal. Each case is an extended output,
        // and implementations should also check that the first 32 bytes match their
        // default-length output.",
        try {
            String jsonStr = String.join("", Files.lines(Paths.get("./src/test/resources/test_vectors.json"))
                                                  .toArray(String[]::new));
            JsonNode json = new ObjectMapper().readTree(jsonStr);

            String contextString = "BLAKE3 2019-12-27 16:29:52 test vectors context";
            String key = json.get("key").asText();

            ArrayNode cases = (ArrayNode) json.get("cases");
            for (int i = 0; i < cases.size(); i++) {
                JsonNode testCase = cases.get(i);
                int inputLen = testCase.get("input_len").asInt();
                byte[] inputData = getTestVectorInput(inputLen);
                Blake3 blake3 = Blake3.newInstance();
                Blake3 keyed = Blake3.newKeyedHasher(key.getBytes(StandardCharsets.US_ASCII));
                Blake3 kdf = Blake3.newKeyDerivationHasher(contextString);

                blake3.update(inputData);
                keyed.update(inputData);
                kdf.update(inputData);

                assertEquals(testCase.get("hash").asText(), blake3.hexdigest(131));
                assertEquals(testCase.get("keyed_hash").asText(), keyed.hexdigest(131));
                assertEquals(testCase.get("derive_key").asText(), kdf.hexdigest(131));

                assertEquals(testCase.get("hash").asText().substring(0, 64), blake3.hexdigest());
                assertEquals(testCase.get("keyed_hash").asText().substring(0, 64), keyed.hexdigest());
                assertEquals(testCase.get("derive_key").asText().substring(0, 64), kdf.hexdigest());
            }
        } catch (IOException e) {
            fail("Json file not found");
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception thrown");
        }
    }

    @Test
    public void testFileHash() {
        try {
            Blake3 hasher = Blake3.newInstance();
            hasher.update(new File(LICENSE));
            assertEquals("8dfca237cb64c3f32cbe2df58fbea61e8ca76259ee124030167e7613e34c237d", hasher.hexdigest());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception thrown");
        }
    }

    @Test
    public void testKDFHash() {
        Blake3 hasher = Blake3.newKeyDerivationHasher("meowmeowverysecuremeowmeow");
        hasher.update(testBytes);
        assertEquals("348de7e5f8f804216998120d1d05c6d233d250bdf40220dbf02395c1f89a73f7", hasher.hexdigest());
    }

    @Test
    public void testKeyedFileHash() {
        try {
            Blake3 hasher = Blake3.newKeyedHasher(testKeyedHashBytes);
            hasher.update(new File(LICENSE));
            assertEquals("012c2f1b7f7e00ed841b86bd4c83d2970b7676525dad9b0d8d1757b27d996444", hasher.hexdigest());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception thrown");
        }
    }

    @Test
    public void testLongerHash() {
        Blake3 hasher = Blake3.newInstance();
        hasher.update(testBytes);
        assertEquals("718b749f12a61257438b2ea6643555fd995001c9d9ff84764f93f82610a780f243a9903464658159cf8b216e79006e12ef3568851423fa7c97002cbb9ca4dc44b4185bb3c6d18cdd1a991c2416f5e929810290b24bf24ba6262012684b6a0c4e096f55e8b0b4353c7b04a1141d25afd71fffae1304a5abf0c44150df8b8d4017",
                     hasher.hexdigest(128));
    }

    @Test
    public void testRawByteHash() {
        Blake3 hasher = Blake3.newInstance();
        hasher.update(testBytes);
        byte[] digest = hasher.digest();
        assertTrue(Arrays.equals(digest,
                                 new byte[] { 113, -117, 116, -97, 18, -90, 18, 87, 67, -117, 46, -90, 100, 53, 85, -3,
                                              -103, 80, 1, -55, -39, -1, -124, 118, 79, -109, -8, 38, 16, -89, -128,
                                              -14 }));
    }

    @Test
    public void testShorterHash() {
        Blake3 hasher = Blake3.newInstance();
        hasher.update(testBytes);
        assertEquals("718b749f12a61257438b2ea6643555fd", hasher.hexdigest(16));
    }
}
