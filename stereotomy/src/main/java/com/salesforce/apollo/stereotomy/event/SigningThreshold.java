/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.util.Optional;

/**
 * @author hal.hildebrand
 *
 */
public interface SigningThreshold {

    interface Unweighted extends SigningThreshold {

        int threshold();

    }

    interface Weighted extends SigningThreshold {

        interface Weight {
            Optional<Integer> denominator();

            int numerator();
        }

        Weight[][] weights();
    }

}
