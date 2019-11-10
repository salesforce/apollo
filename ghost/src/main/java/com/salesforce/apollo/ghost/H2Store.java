/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.ghost.schema.Tables.DAG;
import static com.salesforce.apollo.protocols.Conversion.manifestDag;

import java.util.List;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author hhildebrand
 *
 */
public class H2Store implements Store {

    private static final String PASSWORD  = null;
    private static final String USER_NAME = null;

    private final DSLContext context;

    public H2Store(String dbConnect) {
        final HikariConfig roundConfig = new HikariConfig();
        roundConfig.setMinimumIdle(3_000);
        roundConfig.setMaximumPoolSize(1);
        roundConfig.setUsername(USER_NAME);
        roundConfig.setPassword(PASSWORD);
        roundConfig.setJdbcUrl(dbConnect);
        roundConfig.setAutoCommit(false);
        context = DSL.using(new HikariDataSource(roundConfig), SQLDialect.H2);
    }

    @Override
    public void add(List<DagEntry> entries, List<HASH> total) {
    }

    @Override
    public List<DagEntry> entriesIn(CombinedIntervals combinedIntervals, List<HASH> have) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DagEntry get(HASH key) {
        return context.transactionResult(config -> {
            Record1<byte[]> DagEntry = DSL.using(config)
                                          .select(DAG.DATA)
                                          .from(DAG)
                                          .where(DAG.HASH.eq(key.bytes()))
                                          .fetchOne();
            return manifestDag(DagEntry.value1());
        });
    }

    @Override
    public List<DagEntry> getUpdates(List<HASH> want) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<HASH> have(CombinedIntervals keyIntervals) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<HASH> keySet() {
        return context.transactionResult(config -> {
            return DSL.using(config)
                      .select(DAG.HASH)
                      .from(DAG)
                      .fetchStream()
                      .map(r -> new HASH(r.value1()))
                      .collect(Collectors.toList());
        });
    }

    @Override
    public void put(HASH key, DagEntry value) {
        context.transaction(config -> {
            DSL.using(config)
               .insertInto(DAG, DAG.HASH, DAG.DATA)
               .values(key.bytes(), value.getData().array())
               .execute();
        });
    }

}
