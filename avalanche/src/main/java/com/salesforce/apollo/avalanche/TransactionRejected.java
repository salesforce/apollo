/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

/**
 * @author hal.hildebrand
 *
 */
public class TransactionRejected extends Exception {
 
    public TransactionRejected(String message, Throwable cause) {
        super(message, cause); 
    }

    public TransactionRejected(String message) {
        super(message); 
    }

    public TransactionRejected(Throwable cause) {
        super(cause); 
    }

    private static final long serialVersionUID = 1L;

}
