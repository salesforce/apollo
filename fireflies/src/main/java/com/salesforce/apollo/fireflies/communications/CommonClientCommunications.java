/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies.communications;

import com.salesforce.apollo.fireflies.Member;

abstract public class CommonClientCommunications {

    protected final Member member;

    public CommonClientCommunications(Member member) {
        this.member = member;
    }

    abstract public void close();

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof CommonClientCommunications))
            return false;
        CommonClientCommunications other = (CommonClientCommunications)obj;
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
