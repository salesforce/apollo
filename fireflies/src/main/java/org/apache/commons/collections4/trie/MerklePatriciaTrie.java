/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package org.apache.commons.collections4.trie;

import java.util.Map;

import com.salesforce.apollo.fireflies.HashKeyAnalyzer;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class MerklePatriciaTrie<E> extends AbstractPatriciaTrie<HashKey, E> {

    private static final long serialVersionUID = 4446367780901817838L;

    public MerklePatriciaTrie(int maxLengthInBits) {
        super(new HashKeyAnalyzer());
    }

    public MerklePatriciaTrie(int maxLengthInBits, final Map<byte[], ? extends E> m) {
        super(new HashKeyAnalyzer());
    }

}
