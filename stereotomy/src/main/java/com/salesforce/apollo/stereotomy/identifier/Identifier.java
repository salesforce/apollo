/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface Identifier {

    Identifier NONE = new Identifier() {

        @Override
        public boolean isTransferable() {
            return false;
        }
    };

    boolean isTransferable();
}
