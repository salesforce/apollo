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
public class ContextBridge<T extends Member> implements MembershipListener<T> {

    private final Context<T> managed;
    private volatile UUID    registration;

    public ContextBridge(Context<T> managed) {
        this.managed = managed;
    }

    @Override
    public void active(T member) {
        managed.activateIfMember(member);
    }

    public void deregister(Context<T> overlay) {
        var current = registration;
        if (current != null) {
            overlay.deregister(current);
        }
        registration = null;
    }

    @Override
    public void offline(T member) {
        managed.offlineIfMember(member);
    }

    public void register(Context<T> overlay) {
        var current = registration;
        if (current == null) {
            registration = overlay.register(this);
        }
    }
}
