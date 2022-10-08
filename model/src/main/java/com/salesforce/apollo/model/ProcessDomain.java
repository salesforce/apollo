/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.ViewChangeListener;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.EventValidation;

/**
 * The logical domain of the current "Process" - OS and Simulation defined,
 * 'natch.
 * <p>
 * The ProcessDomain represents a member node in the top level domain and
 * represents the top level container model for the distributed system. This top
 * level domain contains every sub domain as decendents. The membership of this
 * domain is the entirety of all process members in the system. The Context of
 * this domain is also the foundational fireflies membership domain of the
 * entire system.
 * 
 * @author hal.hildebrand
 *
 */
public class ProcessDomain extends Domain {
    private record Managed(SubDomain domain, Context<?> embedded) {}

    private final static Logger log = LoggerFactory.getLogger(ProcessDomain.class);

    private final View                 foundation;
    @SuppressWarnings("unused")
    private final Map<Digest, Managed> hostedDomains = new ConcurrentHashMap<>();
    private final UUID                 listener;

    public ProcessDomain(Digest group, ControlledIdentifierMember member, Builder builder, String dbURL,
                         Path checkpointBaseDir, Parameters.RuntimeParameters.Builder runtime,
                         InetSocketAddress endpoint, com.salesforce.apollo.fireflies.Parameters.Builder ff,
                         TransactionConfiguration txnConfig) {
        super(member, builder, dbURL, checkpointBaseDir, runtime, txnConfig);
        var base = Context.<Participant>newBuilder()
                          .setId(group)
                          .setCardinality(params.runtime().foundation().getFoundation().getMembershipCount())
                          .build();
        this.foundation = new View(base, getMember(), endpoint, EventValidation.NONE, params.communications(),
                                   ff.build(), DigestAlgorithm.DEFAULT, null, params.exec());
        listener = foundation.register(listener());
    }

    public View getFoundation() {
        return foundation;
    }

    public CompletableFuture<CertificateWithPrivateKey> provision(Duration duration,
                                                                  SignatureAlgorithm signatureAlgorithm) {
        return member.getIdentifier().provision(Instant.now(), duration, signatureAlgorithm);
    }

    @Override
    public void stop() {
        super.stop();
        foundation.deregister(listener);
    }

    private ViewChangeListener listener() {
        return (context, id, join, leaving) -> {
            for (var d : join) {
                params.context().activate(context.getMember(d));
            }
            for (var d : leaving) {
                params.context().remove(d);
            }

            log.info("View change: {} for: {} joining: {} leaving: {} on: {}", id, params.context().getId(),
                     join.size(), leaving.size(), params.member().getId());
        };
    }
}
