/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.util.List;
import java.util.Set;

import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

record Bound(Digest view, List<NoteWrapper> seeds, int cardinality, Set<SignedNote> joined, BloomFilter<Digest> bff) {}
