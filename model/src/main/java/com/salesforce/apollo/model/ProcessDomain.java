/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.DynamicContext;
import com.salesforce.apollo.context.ViewChange;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.Verifiers;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.thoth.KerlDHT;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * The logical domain of the current "Process" - OS and Simulation defined, 'natch.
 * <p>
 * The ProcessDomain represents a member node in the top level domain and represents the top level container model for
 * the distributed system. The DynamicContext of this domain is the foundational fireflies membership domain for the
 * group id.
 *
 * @author hal.hildebrand
 */
public class ProcessDomain extends Domain {
    private final static Logger log = LoggerFactory.getLogger(ProcessDomain.class);

    protected final KerlDHT                             dht;
    protected final View                                foundation;
    private final   EventValidation.DelegatedValidation validations;
    private final   Verifiers.DelegatedVerifiers        verifiers;
    private final   ProcessDomainParameters             parameters;
    private final   List<BiConsumer<Context, Digest>>   lifecycleListeners = new CopyOnWriteArrayList<>();
    private final   Consumer<ViewChange>                listener           = listener();

    public ProcessDomain(Digest group, ControlledIdentifierMember member, ProcessDomainParameters pdParams,
                         Builder builder, Parameters.RuntimeParameters.Builder runtime, String endpoint,
                         com.salesforce.apollo.fireflies.Parameters.Builder ff, StereotomyMetrics stereotomyMetrics) {
        super(member, builder, pdParams.dbURL, pdParams.checkpointBaseDir, runtime);
        parameters = pdParams;
        var b = DynamicContext.<Participant>newBuilder();
        b.setBias(parameters.dhtBias).setpByz(parameters.dhtPbyz).setId(group);
        var base = b.build();
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(parameters.dhtDbUrl, "", "");
        connectionPool.setMaxConnections(parameters.jdbcMaxConnections());
        dht = new KerlDHT(parameters.dhtOpsFrequency, params.context(), member, connectionPool,
                          params.digestAlgorithm(), params.communications(), parameters.dhtOperationsTimeout,
                          parameters.dhtFpr, stereotomyMetrics);
        validations = new EventValidation.DelegatedValidation(EventValidation.NONE);
        verifiers = new Verifiers.DelegatedVerifiers(Verifiers.NONE);
        this.foundation = new View(base, getMember(), endpoint, validations, verifiers, params.communications(),
                                   ff.build(), DigestAlgorithm.DEFAULT, null);
        foundation.register("ProcessDomain[%s]".formatted(member.getId()), listener);
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

    public void setAniValidations() {
        validations.setDelegate(dht.getAni().eventValidation(parameters.dhtEventValidTO));
    }

    public void setDhtVerifiers() {
        verifiers.setDelegate(dht.getVerifiers());
    }

    public void setValidationsNONE() {
        validations.setDelegate(EventValidation.NONE);
    }

    public void setVerifiersNONE() {
        verifiers.setDelegate(Verifiers.NONE);
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

    protected Consumer<ViewChange> listener() {
        return (viewChange) -> {
            log.info("Start view change: {} for: {} cardinality: {} on: {}", viewChange.diadem(),
                     params.context().getId(), viewChange.context().size(), params.member().getId());
            choam.rotateViewKeys(viewChange);
            dht.nextView(viewChange);

            log.info("Finished view change: {} for: {} cardinality: {} on: {}", viewChange.diadem(),
                     params.context().getId(), viewChange.context().size(), params.member().getId());
        };
    }

    protected void startServices() {
        dht.start(parameters.kerlSpaceDuration);
    }

    protected void stopServices() {
        dht.stop();
        foundation.stop();
    }

    public record ProcessDomainParameters(String dbURL, Duration dhtOperationsTimeout, String dhtDbUrl,
                                          Path checkpointBaseDir, Duration dhtOpsFrequency, double dhtFpr,
                                          Duration dhtEventValidTO, int dhtBias, Duration kerlSpaceDuration,
                                          int jdbcMaxConnections, double dhtPbyz) {
    }
}
