/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import com.google.protobuf.Any;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.service.Resolver;

/**
 * @author hal.hildebrand
 *
 */
public class Thoth implements Resolver {

    private final Context<Member> context;
    private final SigningMember   node;

    public Thoth(Context<Member> context, SigningMember node) {
        this.context = context;
        this.node = node;
    }

    @Override
    public Any lookup(Identifier prefix) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public KeyState resolve(Identifier prefix) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void bind(Identifier prefix, Any value) {
        // TODO Auto-generated method stub

    }
}
