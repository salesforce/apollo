/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.DriverManager;

import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Database;
import org.jooq.meta.jaxb.Generator;
import org.jooq.meta.jaxb.Jdbc;
import org.jooq.meta.jaxb.Target;

/**
 * @author hal.hildebrand
 *
 */
public class GenerateSchema {

    private static final String JDBC_URL = "jdbc:h2:mem:generate_quantum_schema";

    public static void main(String[] argv) throws Exception {
        Connection connection = DriverManager.getConnection(JDBC_URL);
        QuantumTables.prepare(connection);
        Configuration configuration = new Configuration().withJdbc(new Jdbc().withDriver("org.h2.Driver")
                                                                             .withUrl(JDBC_URL))
                                                         .withGenerator(new Generator().withDatabase(new Database().withName("org.jooq.meta.h2.H2Database")
                                                                                                                   .withIncludes(".*")
                                                                                                                   .withExcludes("")
                                                                                                                   .withInputSchema("QUANTUMDB"))
                                                                                       .withTarget(new Target().withClean(true)
                                                                                                               .withPackageName("com.apollo.qdb.quantumdbSchema")
                                                                                                               .withDirectory("src/main/java")));
        GenerationTool.generate(configuration);
    }

}
