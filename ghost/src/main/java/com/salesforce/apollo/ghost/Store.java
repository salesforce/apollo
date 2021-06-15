/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.util.List;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.Digest; 

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Store {

	public List<Any> entriesIn(CombinedIntervals combinedIntervals, List<Digest> have);

	public List<Digest> have(CombinedIntervals keyIntervals);

	public List<Digest> keySet();

	void add(List<Any> entries, List<Digest> total);

	Any get(Digest key);

	List<Any> getUpdates(List<Digest> want);

	void put(Digest key, Any value);

}
