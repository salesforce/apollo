/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.cryptography.*;
import com.salesforce.apollo.ethereal.Dag.Decoded;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.security.KeyPair;
import java.util.HashMap;
import java.util.Scanner;

/**
 * @author hal.hildebrand
 */
public class DagReader {
    public static final KeyPair DEFAULT_KEYPAIR;

    public static final Signer DEFAULT_SIGNER;

    private final static String KEY_TEMPLATE = "%s-%s-%s";
    private final static Logger log          = LoggerFactory.getLogger(DagReader.class);

    static {
        DEFAULT_KEYPAIR = SignatureAlgorithm.DEFAULT.generateKeyPair();
        DEFAULT_SIGNER = new Signer.SignerImpl(DEFAULT_KEYPAIR.getPrivate(), ULong.MIN);
    }

    public static void add(Dag dag, PreUnit pu) {
        if (pu.epoch() != dag.epoch()) {
            System.out.println(String.format("Failed: %s reason: %s", pu.hash(), Correctness.DATA_ERROR));
        }
        var alreadyInDag = dag.get(pu.hash());
        if (alreadyInDag != null) {
            System.out.println(String.format("Failed: %s reason: %s", pu.hash(), Correctness.DUPLICATE_UNIT));
        }
        Decoded decodedParents = dag.decodeParents(pu);
        if (decodedParents.inError()) {
            System.out.println(String.format("Failed: %s reason: %s", pu.hash(), decodedParents.classification()));
        }
        Unit[] parents = decodedParents.parents();
        var freeUnit = dag.build(pu, parents);
        dag.insert(freeUnit);
    }

    public static Dag readDag(InputStream is, DagFactory df) {
        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(is);
        short n = scanner.nextShort();
        scanner.nextLine();
        Dag dag = df.createDag(n);
        var preUnitHashes = new HashMap<String, Digest>();
        var rsData = new byte[0];
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (line.startsWith("//") || line.isEmpty()) {
                continue;
            }

            short puCreator = -1;
            int puHeight = -1, puVersion = -1;
            short creator = -1;
            int height = -1, version = -1;
            var parents = new Digest[n];
            var parentsHeights = new int[n];
            for (int i = 0; i < n; i++) {
                parentsHeights[i] = -1;
            }
            int i = 0;
            for (var t : line.split(" ")) {
                var node = t.split("-");
                if (node.length != 3) {
                    throw new IllegalStateException("Unable to parse: " + line);
                }
                creator = (short) Integer.parseInt(node[0]);
                height = Integer.parseInt(node[1]);
                version = Integer.parseInt(node[2]);
                if (i++ == 0) {
                    puCreator = creator;
                    puHeight = height;
                    puVersion = version;
                } else {
                    Digest hash = preUnitHashes.get(String.format(KEY_TEMPLATE, creator, height, version));
                    if (hash == null) {
                        throw new IllegalStateException("Trying to set parent to non-existing unit");
                    }
                    if (parents[creator] != null) {
                        throw new IllegalStateException("Duplicate parent");
                    }
                    parents[creator] = hash;
                    parentsHeights[creator] = height;
                }
            }
            var pu = newPreUnit(puCreator, new Crown(parentsHeights, Digest.combine(DigestAlgorithm.DEFAULT, parents)),
                                ByteString.copyFromUtf8(" "), rsData, DigestAlgorithm.DEFAULT);
            add(dag, pu);
            log.debug("insert: {}", pu);
            preUnitHashes.put(String.format(KEY_TEMPLATE, puCreator, puHeight, puVersion), pu.hash());
        }
        return dag;
    }

    private static PreUnit newPreUnit(short puCreator, Crown crown, ByteString data, byte[] rsData,
                                      DigestAlgorithm algo) {
        PreUnit newsie = newPreUnitFromEpoch(0, puCreator, crown, data, algo, DEFAULT_SIGNER);
        return newsie;
    }

    private static PreUnit newPreUnitFromEpoch(int epoch, short puCreator, Crown crown, ByteString data,
                                               DigestAlgorithm algo, Signer signer) {
        byte[] salt = {};
        JohnHancock signature = PreUnit.sign(signer, puCreator, crown, data, salt);
        return new preUnit(puCreator, epoch, crown.heights()[puCreator] + 1, signature.toDigest(algo), crown, data,
                           signature, salt);
    }
}
