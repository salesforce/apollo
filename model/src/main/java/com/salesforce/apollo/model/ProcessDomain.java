/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.ViewLifecycleListener;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.thoth.KerlDHT;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

/**
 * The logical domain of the current "Process" - OS and Simulation defined, 'natch.
 * <p>
 * The ProcessDomain represents a member node in the top level domain and represents the top level container model for
 * the distributed system. This top level domain contains every sub domain as decendents. The membership of this domain
 * is the entirety of all process members in the system. The Context of this domain is also the foundational fireflies
 * membership domain of the entire system.
 *
 * @author hal.hildebrand
 */
public class ProcessDomain extends Domain {

    private final static Logger log = LoggerFactory.getLogger(ProcessDomain.class);

    protected final KerlDHT dht;
    protected final View    foundation;
    private final   UUID    listener;

    public ProcessDomain(Digest group, ControlledIdentifierMember member, Builder builder, String dbURL,
                         Path checkpointBaseDir, Parameters.RuntimeParameters.Builder runtime,
                         InetSocketAddress endpoint, com.salesforce.apollo.fireflies.Parameters.Builder ff,
                         EventValidation eventValidation) {
        super(member, builder, dbURL, checkpointBaseDir, runtime);
        var base = Context.<Participant>newBuilder()
                          .setId(group)
                          .setCardinality(params.runtime().foundation().getFoundation().getMembershipCount())
                          .build();
        this.foundation = new View(base, getMember(), endpoint, eventValidation, params.communications(), ff.build(),
                                   DigestAlgorithm.DEFAULT, null);
        final var url = String.format("jdbc:h2:mem:%s-%s;DB_CLOSE_DELAY=-1", member.getId(), "");
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        connectionPool.setMaxConnections(10);
        dht = new KerlDHT(Duration.ofMillis(10), foundation.getContext(), member, connectionPool,
                          params.digestAlgorithm(), params.communications(), Duration.ofSeconds(1), 0.00125, null);
        listener = foundation.register(listener());
    }

    public KerlDHT getDht() {
        return dht;
    }

    public View getFoundation() {
        return foundation;
    }

    public CertificateWithPrivateKey provision(Duration duration, SignatureAlgorithm signatureAlgorithm) {
        return member.getIdentifier().provision(Instant.now(), duration, signatureAlgorithm);
    }

    @Override
    public void start() {
        startServices();
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        foundation.deregister(listener);
        try {
            stopServices();
        } catch (RejectedExecutionException e) {

        }
    }

    protected ViewLifecycleListener listener() {
        return (context, id, join, leaving) -> {
            for (var d : join) {
                if (d.getIdentifier() instanceof SelfAddressingIdentifier sai) {
                    params.context().activate(context.getMember(sai.getDigest()));
                }
            }
            for (var d : leaving) {
                params.context().remove(d);
            }

            log.info("View change: {} for: {} joining: {} leaving: {} on: {}", id, params.context().getId(),
                     join.size(), leaving.size(), params.member().getId());
        };
    }

    protected void startServices() {
        dht.start(params.gossipDuration());
    }

    protected void stopServices() {
        dht.stop();
    }
}
