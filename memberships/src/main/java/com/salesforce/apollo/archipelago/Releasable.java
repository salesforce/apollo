package com.salesforce.apollo.archipelago;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import io.grpc.ManagedChannel;

public interface Releasable {
    ManagedChannel getChannel();

    Digest getFrom();

    Member getMember();

    void release();

    ManagedChannel shutdown();

    ManagedChannel shutdownNow();
}
