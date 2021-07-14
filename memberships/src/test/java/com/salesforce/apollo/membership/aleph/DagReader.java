/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.membership.aleph.DagFactory.DagAdder;
import com.salesforce.apollo.membership.aleph.PreUnit.preUnit;

/**
 * @author hal.hildebrand
 *
 */
public class DagReader {

    private final static String KEY_TEMPLATE = "%s-%s-%s";

    public static DagAdder readDag(InputStream is, DagFactory df) {
        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(is);
        short n = (short) scanner.nextShort();
        scanner.nextLine();
        DagAdder da = df.createDag(n);
        var preUnitHashes = new HashMap<String, Digest>();
        JohnHancock signature = new JohnHancock(SignatureAlgorithm.DEFAULT, new byte[0]);
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
            var parents = new ArrayList<Digest>();
            var parentsHeights = new ArrayList<Integer>();
            for (int i = 0; i < n; i++) {
                parents.add(null);
                parentsHeights.add(-1);
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
                    if (parents.get(creator) != null) {
                        throw new IllegalStateException("Duplicate parent");
                    }
                    parents.set(creator, hash);
                    parentsHeights.set(creator, height);
                }
            }
            var pu = newPreUnit(puCreator, new Crown(parentsHeights, Digest.combine(DigestAlgorithm.DEFAULT, parents)),
                                Any.getDefaultInstance(), rsData, signature, DigestAlgorithm.DEFAULT);
            var errors = da.adder().addPreunits(pu.creator(), Collections.singletonList(pu));
            if (errors != null) {
                throw new IllegalStateException("Unable to insert node: " + pu + " : " + errors);
            }
            preUnitHashes.put(String.format(KEY_TEMPLATE, puCreator, puHeight, puVersion), pu.hash());
        }
        return da;
    }

    private static PreUnit newPreUnit(short puCreator, Crown crown, Any defaultInstance, byte[] rsData,
                                      JohnHancock signature, DigestAlgorithm default1) {
        PreUnit newsie = newPreUnitFromEpoch(0, puCreator, crown, defaultInstance, rsData, signature, default1);
        return newsie;
    }

    private static PreUnit newPreUnitFromEpoch(int epoch, short puCreator, Crown crown, Any defaultInstance,
                                               byte[] rsData, JohnHancock signature, DigestAlgorithm default1) {
        return new preUnit(puCreator, epoch, crown.heights().get(puCreator) + 1, signature,
                           PreUnit.computeHash(default1, puCreator, crown, defaultInstance, rsData), crown,
                           defaultInstance, rsData);
    }
}
