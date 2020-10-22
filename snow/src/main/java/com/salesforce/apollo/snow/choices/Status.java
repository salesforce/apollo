/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.choices;

/**
 * @author hal.hildebrand
 *
 */
public enum Status {
    UNKNOWN, PROCESSING {
        boolean fetched() {
            return true;
        }
    },
    REJECTED {
        boolean decided() {
            return true;
        }
    },
    ACCEPTED {
        boolean decided() {
            return true;
        }
    };

    boolean fetched() {
        return decided();
    }

    boolean decided() {
        return false;
    }
}
