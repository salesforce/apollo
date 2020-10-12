/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.bootstrap;

import static com.salesforce.apollo.boostrap.schema.Tables.ASSIGNED_IDS;
import static com.salesforce.apollo.boostrap.schema.Tables.SETTINGS;
import static io.github.olivierlemasle.ca.CA.dn;

import java.security.SecureRandom;

import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hhildebrand
 */
public class BootstrapCA extends Application<BootstrapConfiguration> {

    private final SecureRandom entropy = new SecureRandom();

    public static void main(String[] argv) throws Exception {
        new BootstrapCA().run(argv);
    }

    private DSLContext context;

    public DSLContext getContext() {
        return context;
    }

    @Override
    public void run(BootstrapConfiguration configuration, Environment environment) throws Exception {
        RootCertificate root = MintApi.mint(dn().setCn("test-ca.com")
                                                .setO("World Company")
                                                .setOu("IT dep")
                                                .setSt("CA")
                                                .setC("US")
                                                .build(),
                                            configuration.cardinality, configuration.probabilityByzantine,
                                            configuration.faultTolerance);
        context = DSL.using(new H2PooledConnectionProvider(
                JdbcConnectionPool.create(configuration.dbConnect, "bootstrap", "")), SQLDialect.H2);
        MintApi.loadSchema(context);
        initializeDb(configuration, root);
        environment.jersey()
                   .register(new MintApi(root, context,
                           (int) ((configuration.cardinality * configuration.faultTolerance) + 1)));
        environment.healthChecks().register("bootstrap", new BoostrapCaHealthCheck());
    }

    private void initializeDb(BootstrapConfiguration configuration, RootCertificate root) {
        context.transaction(config -> {
            DSLContext create = DSL.using(config);
            create.insertInto(SETTINGS)
                  .set(SETTINGS.VERSION, 0)
                  .set(SETTINGS.CARDINALITY, configuration.cardinality)
                  .set(SETTINGS.PROBABILITY_BYZANTINE, configuration.probabilityByzantine)
                  .set(SETTINGS.FAULTTOLERANCE, configuration.faultTolerance)
                  .set(SETTINGS.CA_CERTIFICATE, root.getX509Certificate().getEncoded())
                  .execute();

            for (int i = 0; i < configuration.cardinality; i++) {
                byte[] id = new byte[32];
                entropy.nextBytes(id);
                create.insertInto(ASSIGNED_IDS).set(ASSIGNED_IDS.ID, id).execute();
            }
        });
    }

}
