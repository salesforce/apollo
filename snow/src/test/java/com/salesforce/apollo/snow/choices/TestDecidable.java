/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.choices;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class TestDecidable implements Decidable {

    public ID        idV;
    public Throwable acceptV;
    public Throwable rejectV;
    public Status    statusV;

    @Override
    public ID id() {
        return idV;
    }

    @Override
    public void accept() {
        switch (statusV) {
        case UNKNOWN:
        case REJECTED:
            throw new IllegalStateException(
                    String.format("invalid state transition from %s to %s", statusV, Status.ACCEPTED));
        default:
            statusV = Status.ACCEPTED;
        }
    }

    @Override
    public void reject() {
        switch (statusV) {
        case UNKNOWN:
        case ACCEPTED:
            throw new IllegalStateException(
                    String.format("invalid state transaition from %s to %s", statusV, Status.REJECTED));
        default:
            statusV = Status.REJECTED;
        }
    }

    @Override
    public Status status() {
        return statusV;
    }

}
