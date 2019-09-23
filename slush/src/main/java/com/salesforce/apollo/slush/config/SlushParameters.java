/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush.config;

/**
 * The parameters of the Slush protocol
 */
public class SlushParameters extends Parameters {
    /**
     * The number of rounds - this determines the diffusion of the queries across the member network
     */
    public int rounds; 
}