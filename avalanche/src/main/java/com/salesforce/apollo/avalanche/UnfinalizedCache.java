/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.salesforce.apollo.avro.HASH;

/**
 * @author hhildebrand
 *
 */
public class UnfinalizedCache {

    public static class Node {

    }

    private final ConcurrentMap<HASH, Node> unfinalized = new ConcurrentHashMap<>();

}
