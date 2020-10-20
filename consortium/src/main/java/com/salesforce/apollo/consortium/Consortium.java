/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.security.SecureRandom;

import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.AvalancheMetrics;
import com.salesforce.apollo.avalanche.AvalancheParameters;
import com.salesforce.apollo.avalanche.Processor;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Consortium implements Processor {
    private class Leader extends Committee implements Processor {

        @Override
        public void fail(HashKey txn) {
            // TODO Auto-generated method stub

        }

        @Override
        public void finalize(HashKey txn) {
            // TODO Auto-generated method stub

        }

    }

    private class Follower extends Committee implements Processor {

        @Override
        public void fail(HashKey txn) {
            // TODO Auto-generated method stub

        }

        @Override
        public void finalize(HashKey txn) {
            // TODO Auto-generated method stub

        }

    }

    private class Client implements Processor {

        @Override
        public void fail(HashKey txn) {
            // TODO Auto-generated method stub

        }

        @Override
        public void finalize(HashKey txn) {
            // TODO Auto-generated method stub

        }

    }

    @SuppressWarnings("unused")
    private final Avalanche    avalanche;
    private volatile Processor delegate = new Client();

    public Consortium(Node node, com.salesforce.apollo.membership.Context<? extends Member> context,
            SecureRandom entropy, Communications communications, AvalancheParameters p, AvalancheMetrics metrics) {
        avalanche = new Avalanche(node, context, entropy, communications, p, metrics, this);

    }

    @Override
    public void fail(HashKey txn) {
        getDelegate().fail(txn);
    }

    @Override
    public void finalize(HashKey txn) {
        getDelegate().finalize(txn);
    }

    public void setAvalanche(Avalanche avalanche) {
        // ignore
    }

    private Processor getDelegate() {
        final Processor get = delegate;
        return get;
    }

    @SuppressWarnings("unused")
    private void becomeLeader() {
        delegate = new Leader();
    }

    @SuppressWarnings("unused")
    private void becomeFollower() {
        delegate = new Follower();
    }

    @SuppressWarnings("unused")
    private void becomeClient() {
        delegate = new Client();
    }
}
