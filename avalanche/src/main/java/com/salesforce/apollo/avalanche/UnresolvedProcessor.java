/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

/**
 * @author hhildebrand
 */
public class UnresolvedProcessor extends Exception {

    private static final long serialVersionUID = 1L;

    public UnresolvedProcessor(String processor, Throwable e) {
        super("Unresolved entry processor class: " + processor, e);
    }

}
