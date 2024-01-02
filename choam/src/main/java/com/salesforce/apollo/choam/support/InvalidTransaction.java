/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

/**
 * @author hal.hildebrand
 */
public class InvalidTransaction extends Exception {

    private static final long serialVersionUID = 1L;

    public InvalidTransaction() {
    }

    public InvalidTransaction(String message) {
        super(message);
    }

    public InvalidTransaction(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidTransaction(Throwable cause) {
        super(cause);
    }

}
