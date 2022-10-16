/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import java.io.Closeable;

import com.salesforce.apollo.membership.Member;

/**
 * 
 * A client side link
 *
 * @author hal.hildebrand
 *
 */
public interface Link extends Closeable {

    public Member getMember();
}
