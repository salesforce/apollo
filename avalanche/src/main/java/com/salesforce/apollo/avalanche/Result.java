/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

public enum Result {
    FALSE {

        @Override
        public
        Boolean value() {
            return Boolean.FALSE;
        }
    },
    TRUE {

        @Override
        public
        Boolean value() {
            return Boolean.TRUE;
        }
    },
    UNKNOWN {

        @Override
        public
        Boolean value() {
            return null;
        }
    };

    public abstract Boolean value();
}