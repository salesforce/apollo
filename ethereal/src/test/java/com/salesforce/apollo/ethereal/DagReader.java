/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.DagFactory.DagAdder;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;

/**
 * @author hal.hildebrand
 *
 */
public class DagReader {
    private final static Logger log = LoggerFactory.getLogger(DagReader.class);

    private final static String KEY_TEMPLATE = "%s-%s-%s";

    public static DagAdder readDag(InputStream is, DagFactory df) {
        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(is);
        short n = (short) scanner.nextShort();
        scanner.nextLine();
        DagAdder da = df.createDag(n);
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
                                Any.getDefaultInstance(), rsData, DigestAlgorithm.DEFAULT);
            var errors = da.adder().addPreunits(pu.creator(), Collections.singletonList(pu));
            if (errors != null) {
                log.warn("Error on insert: {} : {}", errors.get(pu.hash()), pu);
            } else {
                preUnitHashes.put(String.format(KEY_TEMPLATE, puCreator, puHeight, puVersion), pu.hash());
            }
        }
        return da;
    }

    private static PreUnit newPreUnit(short puCreator, Crown crown, Any defaultInstance, byte[] rsData,
                                      DigestAlgorithm default1) {
        PreUnit newsie = newPreUnitFromEpoch(0, puCreator, crown, defaultInstance, rsData, default1);
        return newsie;
    }

    private static PreUnit newPreUnitFromEpoch(int epoch, short puCreator, Crown crown, Any defaultInstance,
                                               byte[] rsData, DigestAlgorithm default1) {
        return new preUnit(puCreator, epoch, crown.heights()[puCreator] + 1,
                           PreUnit.computeHash(default1, puCreator, crown, defaultInstance, rsData), crown,
                           defaultInstance, rsData);
    }
}
