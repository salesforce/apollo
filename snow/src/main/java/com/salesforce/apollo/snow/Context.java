/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public class Context {

    public static class Builder {
        private ID              avaxAssetID;
        private ID              chainID;
        private EventDispatcher consensusDispatcher;
        private EventDispatcher decisionDispatcher;
        private Logger          log;
        private MetricRegistry  metrics;
        private String          namespace;
        private int             networkID;
        private ShortID         nodeID;
        private ID              subnetID;
        private ID              xchainID;

        public Context build() {
            return new Context(networkID, subnetID, chainID, nodeID, xchainID, avaxAssetID, log, namespace, metrics,
                    decisionDispatcher, consensusDispatcher);
        }

        public ID getAvaxAssetID() {
            return avaxAssetID;
        }

        public ID getChainID() {
            return chainID;
        }

        public EventDispatcher getConsensusDispatcher() {
            return consensusDispatcher;
        }

        public EventDispatcher getDecisionDispatcher() {
            return decisionDispatcher;
        }

        public Logger getLog() {
            return log;
        }

        public MetricRegistry getMetrics() {
            return metrics;
        }

        public String getNamespace() {
            return namespace;
        }

        public int getNetworkID() {
            return networkID;
        }

        public ShortID getNodeID() {
            return nodeID;
        }

        public ID getSubnetID() {
            return subnetID;
        }

        public ID getXchainID() {
            return xchainID;
        }

        public Builder setAvaxAssetID(ID avaxAssetID) {
            this.avaxAssetID = avaxAssetID;
            return this;
        }

        public Builder setChainID(ID origin) {
            this.chainID = origin;
            return this;
        }

        public Builder setConsensusDispatcher(EventDispatcher consensusDispatcher) {
            this.consensusDispatcher = consensusDispatcher;
            return this;
        }

        public Builder setDecisionDispatcher(EventDispatcher decisionDispatcher) {
            this.decisionDispatcher = decisionDispatcher;
            return this;
        }

        public Builder setLog(Logger log) {
            this.log = log;
            return this;
        }

        public Builder setMetrics(MetricRegistry metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder setNamespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder setNetworkID(int networkID) {
            this.networkID = networkID;
            return this;
        }

        public Builder setNodeID(ShortID nodeID) {
            this.nodeID = nodeID;
            return this;
        }

        public Builder setSubnetID(ID subnetID) {
            this.subnetID = subnetID;
            return this;
        }

        public Builder setXchainID(ID xchainID) {
            this.xchainID = xchainID;
            return this;
        }
    }

    public final ID              avaxAssetID;
    public final ID              chainID;
    public final EventDispatcher consensusDispatcher;
    public final EventDispatcher decisionDispatcher;
    public final Logger          log;
    public final MetricRegistry  metrics;
    public final String          namespace;
    public final int             networkID;
    public final ShortID         nodeID;
    public final ID              subnetID;
    public final ID              xchainID;
    private final AtomicBoolean  bootstrapped = new AtomicBoolean();

    public Context(int networkID, ID subnetID2, ID chainID2, ShortID nodeID, ID xchainID, ID avaxAssetID, Logger log,
            String namespace, MetricRegistry metrics, EventDispatcher decisionDispatcher,
            EventDispatcher consensusDispatcher) {
        this.networkID = networkID;
        this.subnetID = subnetID2;
        this.chainID = chainID2;
        this.nodeID = nodeID;
        this.xchainID = xchainID;
        this.avaxAssetID = avaxAssetID;
        this.log = log;
        this.namespace = namespace;
        this.metrics = metrics;
        this.consensusDispatcher = consensusDispatcher;
        this.decisionDispatcher = decisionDispatcher;
    }

    public void bootstrap() {
        bootstrapped.set(true);
    }

    public boolean isBootstrapped() {
        return bootstrapped.get();
    }

    public static Builder newBuilder() {
        return new Builder();
    }
}
