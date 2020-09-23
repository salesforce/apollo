/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import com.salesforce.apollo.membership.Member;

abstract public class CommonClientCommunications {

    protected final Member member;

    public CommonClientCommunications(Member member) {
        this.member = member;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof CommonClientCommunications))
            return false;
        CommonClientCommunications other = (CommonClientCommunications) obj;
        return member.equals(other.member);
    }

    public Member getMember() {
        return member;
    }

    @Override
    public int hashCode() {
        return member.hashCode();
    }

}
