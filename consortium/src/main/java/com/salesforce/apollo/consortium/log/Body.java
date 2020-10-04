/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.log;

import java.util.List;

/**
 * @author hal.hildebrand
 *
 */
public class Body {
    private long              consensusId;
    private List<Transaction> transactions;
    private List<Proof> proofs;
    private List<Response>    responses;
}
