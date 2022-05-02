/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.util.UUID;

import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ContextBridge implements MembershipListener<Member> {

    private final Context managed;
    private volatile UUID registration;

    public ContextBridge(Context<? extends Member> managed) {
        this.managed = managed;
    }

    @Override
    public void active(Member member) {
        managed.activate(member);
    }

    public void deregister(Context overlay) {
        var current = registration;
        if (current != null) {
            overlay.deregister(current);
        }
        registration = null;
    }

    @Override
    public void offline(Member member) {
        managed.offline(member);
    }

    public void register(Context context) {
        var current = registration;
        if (current == null) {
            registration = context.register(this);
        }
    }
}
