/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

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
    private record Managed<T extends Member> (SubDomain domain, Context<T> embedded, ContextBridge<T> bridge) {}

    @SuppressWarnings("unused")
    private final Map<Digest, Managed<?>> hostedDomains = new ConcurrentHashMap<>();
    @SuppressWarnings("unused")
    private final View                    foundation;

    public ProcessDomain(ControlledIdentifier<SelfAddressingIdentifier> id, Builder builder, String dbURL,
                         Path checkpointBaseDir, Parameters.RuntimeParameters.Builder runtime) {
        super(id, builder, dbURL, checkpointBaseDir, runtime);
//        var base = Context.<Participant>newBuilder()
//                          .setCardinality(params.runtime().foundation().getFoundation().getMembershipCount())
//                          .build();
//        foundation = new View(base, new Node(params.member(), null, null), null, null, null);
        foundation = null;
    }

    public Optional<CertificateWithPrivateKey> provision(Duration duration, SignatureAlgorithm signatureAlgorithm) {
        return identifier.provision(Instant.now(), duration, signatureAlgorithm);
    }
}
