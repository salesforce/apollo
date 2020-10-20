/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.grpc.ConsortiumClientCommunications.getCreate;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesforce.apollo.avalanche.AvaMetrics;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.AvalancheParameters;
import com.salesforce.apollo.avalanche.Processor;
import com.salesforce.apollo.comm.CommonCommunications;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.consortium.grpc.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.grpc.ConsortiumServerCommunications;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Consortium implements Processor {
    private final static Logger log = LoggerFactory.getLogger(Consortium.class);

    public class Service {

    }

    private class Client implements State {

    }

    private class Follower extends CommitteeMember implements State {

    }

    private class Leader extends CommitteeMember implements State {

    }

    private interface State {

    }

    private final Avalanche                                            avalanche;
    @SuppressWarnings("unused")
    private volatile Block                                             current;
    @SuppressWarnings("unused")
    private final List<HashKey>                                        currentView = new ArrayList<>();
    private volatile State                                             state       = new Client();
    private final CommonCommunications<ConsortiumClientCommunications> comm;
    private final Service                                              service     = new Service();

    public Consortium(Node node, Context<? extends Member> context, SecureRandom entropy, Communications communications,
            AvalancheParameters p, MetricRegistry metrics) {
        avalanche = new Avalanche(node, context, entropy, communications, p,
                metrics == null ? null : new AvaMetrics(metrics), this);
        comm = communications.create(node, getCreate((ConsortiumMetrics) null), new ConsortiumServerCommunications(
                service, communications.getClientIdentityProvider(), null));

    }

    @SuppressWarnings("unused")
    private ConsortiumClientCommunications linkFor(Participant m) {
        try {
            return comm.apply(m, avalanche.getNode());
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    @Override
    public void fail(HashKey txn) {

    }

    @Override
    public void finalize(HashKey txn) {
    }

    public void setAvalanche(Avalanche avalanche) {
        // ignore
    }

    @SuppressWarnings("unused")
    private void becomeClient() {
        state = new Client();
    }

    @SuppressWarnings("unused")
    private void becomeFollower() {
        state = new Follower();
    }

    @SuppressWarnings("unused")
    private void becomeLeader() {
        state = new Leader();
    }

    @SuppressWarnings("unused")
    private State getState() {
        final State get = state;
        return get;
    }
}
