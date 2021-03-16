/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package io.quantumdb.core.planner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.h2.api.Trigger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Operator;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * @author hal.hildebrand
 *
 */
public abstract class SyncProto implements Trigger {

    private static String TEMPLATE = """
            public class %s extends io.quantumdb.core.planner.SyncProto {
                public %s() {
                    super(%s, %s, %s, %s);
                }
            }
            """;

    public static String generate(String functionName, String schema, String target, List<String> targetFields,
                                  Map<String, Integer> identities) {
        return String.format(TEMPLATE, functionName, functionName, schema, target, asList(targetFields),
                             asMap(identities));
    }

    private static String asList(List<String> targetFields) {
        StringBuilder builder = new StringBuilder();
        builder.append("java.util.List.of(");
        int[] count = new int[] { targetFields.size() };
        targetFields.forEach(f -> {
            builder.append(f);
            if (--count[0] != 0) {
                builder.append(',');
            }
        });
        builder.append(')');
        return builder.toString();
    }

    private static String asMap(Map<String, Integer> identities) {
        StringBuilder builder = new StringBuilder();
        builder.append("java.util.Map.of(");
        int[] count = new int[] { identities.size() };
        identities.entrySet().forEach(e -> {
            builder.append(e.getKey());
            builder.append(',');
            builder.append(e.getValue());
            if (--count[0] != 0) {
                builder.append(',');
            }
        });
        builder.append(')');
        return builder.toString();
    }

    private final Map<Field<Object>, Integer> identities;
    private final Table<?>                    target;
    private final List<Field<?>>              targetFields;
    private int                               type;

    @SuppressWarnings("unchecked")
    public SyncProto(String schema, String target, List<String> targetFields, Map<String, Integer> identities) {
        this.target = DSL.table(DSL.name(schema, target));
        this.targetFields = targetFields.stream().map(f -> this.target.field(f)).collect(Collectors.toList());
        this.identities = identities.entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(e -> (Field<Object>) this.target.field(e.getKey()),
                                                              e -> e.getValue()));
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        DSLContext context = DSL.using(conn, SQLDialect.H2);
        switch (type) {
        case Trigger.INSERT: {
            context.insertInto(target, targetFields).values(List.of(newRow)).execute();
            break;
        }
        case Trigger.UPDATE: {
            if (context.selectCount().from(target).where(condition(oldRow)).fetchOne().value1() == 0) {
                context.insertInto(target, targetFields).values(List.of(newRow)).execute();
            }
            break;
        }
        case Trigger.DELETE: {
            context.deleteFrom(target).where(condition(oldRow));
        }
        default:
            throw new IllegalArgumentException("Invalid trigger type: " + type);
        }
    }

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
                     int type) throws SQLException {
        this.type = type;
    }

    @Override
    public void remove() throws SQLException {
    }

    private Condition condition(Object[] oldRow) {
        Condition condition = DSL.condition(true);
        if (identities.isEmpty()) {
            return condition;
        }
        List<Condition> conditions = new ArrayList<>();
        for (Map.Entry<Field<Object>, Integer> entry : identities.entrySet()) {
            conditions.add(entry.getKey().eq(oldRow[entry.getValue()]));
        }
        return condition.and(DSL.condition(Operator.AND, conditions));
    }
}
