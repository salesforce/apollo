/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;
import com.salesforce.apollo.snow.validators.Validator;

/**
 * @author hal.hildebrand
 *
 */
public class Bootstrapper implements Engine {

    // MaxContainersPerMultiPut is the maximum number of containers that can be
    // sent in a MultiPut message
    public static final int MaxContainersPerMultiPut = 2000;

    // StatusUpdateFrequency is how many containers should be processed between
    // logs
    public static int StatusUpdateFrequency = 2500;

    // MaxOutstandingRequests is the maximum number of GetAncestors sent but not
    // responded to/failed
    public static int MaxOutstandingRequests = 8;

    // MaxTimeFetchingAncestors is the maximum amount of time to spend fetching
    // vertices during a call to GetAncestors
    public static Duration MaxTimeFetchingAncestors = Duration.ofMillis(50);

    private final Set<ID>       acceptedFrontier        = new HashSet<>();
    private final Map<ID, Long> acceptedVotes           = new HashMap<ID, Long>();
    private final Config        config;
    private final Set<ShortID>  pendingAccepted         = new HashSet<>();
    private final Set<ShortID>  pendingAcceptedFrontier = new HashSet<>();
    private int                 requestID;
    private boolean             started;
    private long                weight;
    private Sender              sender;

    public Bootstrapper(Config config) {
        this.config = config;

        Collection<Validator> beacons = config.beacons.sample(config.sampleK);

        for (Validator vdr : beacons) {
            pendingAcceptedFrontier.add(vdr.id());
            pendingAccepted.add(vdr.id());
        }

        if (config.alpha > 0) {
            return;
        }
        startUp();
    }

    @Override
    public void accepted(ShortID validatorID, int requestID, Set<ID> containerIDs) {
        if (!pendingAccepted.contains(validatorID)) {
            config.ctx.log.debug("Received an Accepted message from {} unexpectedly", validatorID);
            return;
        }
        // Mark that we received a response from [validatorID]
        pendingAccepted.remove(validatorID);

        Long weight = config.beacons.getWeight(validatorID);
        if (weight == null) {
            weight = 0L;
        }

        for (ID containerID : containerIDs) {
            Long previousWeight = acceptedVotes.get(containerID);
            long newWeight;
            try {
                newWeight = Math.addExact(weight, previousWeight);
            } catch (ArithmeticException e) {
                newWeight = 0xFFFFFFFF;
            }
            acceptedVotes.put(containerID, newWeight);
        }

        if (pendingAccepted.isEmpty()) {
            return;
        }

        // We've received the filtered accepted frontier from every bootstrap validator
        // Accept all containers that have a sufficient weight behind them
        HashSet<ID> accepted = new HashSet<ID>();
        for (Entry<ID, Long> entry : acceptedVotes.entrySet()) {
            if (entry.getValue() >= config.alpha) {
                accepted.add(entry.getKey());
            }
        }

        if (accepted.isEmpty() && !config.beacons.isEmpty()) {
            config.ctx.log.info("Bootstrapping finished with no accepted frontier. This is likely a result of failing to be able to connect to the specified bootstraps, or no transactions have been issued on this chain yet");
        } else {
            config.ctx.log.info("Bootstrapping started syncing with {} vertices in the accepted frontier",
                                accepted.size());
        }

        config.bootstrapable.forceAccepted(accepted);
    }

    @Override
    public void acceptedFrontier(ShortID validatorID, int requestId, Set<ID> containerIDs) {
        if (!pendingAcceptedFrontier.contains(validatorID)) {
            config.ctx.log.debug("Received an AcceptedFrontier message from {} unexpectedly", validatorID);
            return;
        }
        // Mark that we received a response from [validatorID]
        pendingAcceptedFrontier.remove(validatorID);

        // Union the reported accepted frontier from [validatorID] with the accepted
        // frontier we got from others
        acceptedFrontier.addAll(containerIDs);

        // We've received the accepted frontier from every bootstrap validator
        // Ask each bootstrap validator to filter the list of containers that we were
        // told are on the accepted frontier such that the list only contains containers
        // they think are accepted
        if (pendingAcceptedFrontier.isEmpty()) {
            HashSet<ShortID> vdrs = new HashSet<>(pendingAccepted);

            requestID++;
            sender.getAccepted(vdrs, requestID, acceptedFrontier);
        }
    }

    @Override
    public void chits(ShortID validatorID, int requestID, Set<ID> containerIDs) {
        // TODO Auto-generated method stub

    }

    @Override
    public void connectected(ShortID validatorID) {
        if (started) {
            return;
        }
        Long weight = config.beacons.getWeight(validatorID);
        if (weight == null) {
            return;
        }
        weight = Math.addExact(weight, this.weight);
        this.weight = weight;
        if (weight < config.startupAlpha) {
            return;
        }
        startUp();
    }

    @Override
    public void disconnected(ShortID validatorID) {
        Long weight = config.beacons.getWeight(validatorID);

        if (weight != null) {
            // TODO: Account for weight changes in a more robust manner.

            // Sub64 should rarely error since only validators that have added their
            // weight can become disconnected. Because it is possible that there are
            // changes to the validators set, we utilize that Sub64 returns 0 on
            // error.
            this.weight = Math.subtractExact(this.weight, weight);
        }
    }

    @Override
    public void get(ShortID validatorId, int requestId, ID containerID) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getAccepted(ShortID validatorID, int requestID, Set<ID> containerIDs) {
        sender.accepted(validatorID, requestID, config.bootstrapable.filterAccepted(containerIDs));
    }

    public void getAcceptedFailed(ShortID validatorID, int requestID) {
        // If we can't get a response from [validatorID], act as though they said
        // that they think none of the containers we sent them in GetAccepted are
        // accepted
        accepted(validatorID, requestID, Collections.emptySet());
    }

    @Override
    public void getAcceptedFailure(ShortID validatorID, int requestID) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getAcceptedFrontier(ShortID validatorID, int requestID) {
        sender.acceptedFrontier(validatorID, requestID, config.bootstrapable.currentAcceptedFrontier());
    }

    @Override
    public void getAcceptedFrontierFailed(ShortID validatorID, int requestID) {
        // If we can't get a response from [validatorID], act as though they said their
        // accepted frontier is empty
        acceptedFrontier(validatorID, requestID, Collections.emptySet());
    }

    @Override
    public void getAncestors(ShortID validatorId, int requestID, ID containerID) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getAncestorsFailed(ShortID validatorID, int requestID) {
        // TODO Auto-generated method stub

    }

    @Override
    public Context getContext() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void getFailed(ShortID validatorID, int requestID) {
        // TODO Auto-generated method stub

    }

    @Override
    public void gossip() {
        // TODO Auto-generated method stub

    }

    @Override
    public Object health() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isBootstrapped() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void multiPut(ShortID validatorID, int requestId, byte[][] containers) {
        // TODO Auto-generated method stub

    }

    @Override
    public void Notify() {
        // TODO Auto-generated method stub

    }

    @Override
    public void pullQuery(ShortID validatorID, int requestID, ID containerID) {
        // TODO Auto-generated method stub

    }

    @Override
    public void pushQuery(ShortID validatorID, int requestID, ID containerID, byte[] container) {
        // TODO Auto-generated method stub

    }

    @Override
    public void put(ShortID validatorID, int requestID, ID containerID, byte[] container) {
        // TODO Auto-generated method stub

    }

    @Override
    public void queryFailed(ShortID validatorID, int requestId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

    }

    @Override
    public void startUp() {
        started = true;
        if (pendingAcceptedFrontier.isEmpty()) {
            config.ctx.log.info("Bootstrapping skipped due to no provided bootstraps");
            config.bootstrapable.forceAccepted(Collections.emptySet());
            return;
        }

        // Ask each of the bootstrap validators to send their accepted frontier
        Set<ShortID> vdrs = new HashSet<>(pendingAcceptedFrontier);

        requestID++;
        sender.getAcceptedFrontier(vdrs, requestID);
    }
}
