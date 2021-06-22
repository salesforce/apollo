/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.util.concurrent.Executor;

import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 *
 */
public class RingIterator<Comm extends Link> extends RingCommunications<Comm> {

    public RingIterator(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
            Executor executor) {
        super(context, member, comm, executor); 
    }

    public RingIterator(Direction direction, Context<Member> context, SigningMember member,
            CommonCommunications<Comm, ?> comm, Executor executor) {
        super(direction, context, member, comm, executor); 
    }
  

}
