/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.validators;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public interface Validator {
    class AbstractValidator implements Validator {

        private final ID      id;
        private volatile long weight;

        public AbstractValidator(ID id, long weight) {
            this.id = id;
            this.weight = weight;
        }

        @Override
        public ID id() {
            return id;
        }

        @Override
        public long weight() {
            long current = weight;
            return current;
        }

        void addWeight(long w) {
            try {
                weight = Math.addExact(weight, w);
            } catch (ArithmeticException e) {
                weight = Long.MAX_VALUE;
            }
        }

        void removeWeight(long w) {
            try {
                weight = Math.subtractExact(weight, w);
            } catch (ArithmeticException e) {
                weight = 0;
            }
        }

    }

    ID id();

    long weight();
}
