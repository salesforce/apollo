/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import java.util.Iterator;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Enumerable<T> {

      Iterator<Color> enumerate();
      
      T value();
}
