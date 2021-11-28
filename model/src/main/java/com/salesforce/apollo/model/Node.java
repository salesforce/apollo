/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 *
 */
public class Node {

    private final SigningMember member;
    private final Shard      management;

    public Node(SigningMember member, Shard management) {
        this.member = member;
        this.management = management;
    }

    public Digest getId() {
        return member.getId();
    }
}
