/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLType;
import java.util.List;

import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.value.Value;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.state.proto.Arguments;
import com.salesfoce.apollo.state.proto.Batch;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesfoce.apollo.state.proto.BatchedTransaction;
import com.salesfoce.apollo.state.proto.Call;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesfoce.apollo.state.proto.Txn;

/**
 * @author hal.hildebrand
 *
 */
public final class Helper {
    public static final DataHandler NULL_HANDLER = (DataHandler) Proxy.newProxyInstance(DataHandler.class.getClassLoader(),
                                                                                        new Class<?>[] { DataHandler.class },
                                                                                        new InvocationHandler() {

                                                                                            @Override
                                                                                            public Object invoke(Object proxy,
                                                                                                                 Method method,
                                                                                                                 Object[] args) throws Throwable {
                                                                                                return null;
                                                                                            }
                                                                                        });

    public static BatchedTransaction batch(Message... messages) {
        BatchedTransaction.Builder builder = BatchedTransaction.newBuilder();
        for (Message message : messages) {
            if (message instanceof Call) {
                builder.addTransactions(Txn.newBuilder().setCall((Call) message));
            } else if (message instanceof Batch) {
                builder.addTransactions(Txn.newBuilder().setBatch((Batch) message));
            } else if (message instanceof BatchUpdate) {
                builder.addTransactions(Txn.newBuilder().setBatchUpdate((BatchUpdate) message));
            } else if (message instanceof Statement) {
                builder.addTransactions(Txn.newBuilder().setStatement((Statement) message));
            } else {
                throw new IllegalArgumentException("Unknown transaction batch element type: " + message.getClass());
            }
        }
        return builder.build();
    }

    public static Batch batch(String... statements) {
        Batch.Builder builder = Batch.newBuilder();
        for (String sql : statements) {
            builder.addStatements(sql);
        }
        return builder.build();
    }

    public static BatchUpdate batch(String sql, List<List<Value>> batch) {
        BatchUpdate.Builder builder = BatchUpdate.newBuilder().setSql(sql);
        Data data = Data.create(NULL_HANDLER, 1024, false);
        for (List<Value> arguments : batch) {
            Arguments.Builder args = Arguments.newBuilder();
            for (Value argument : arguments) {
                int valueLen = Data.getValueLen(argument, false);
                data.checkCapacity(valueLen);
                data.writeValue(argument);
                byte[] serialized = data.getBytes();
                args.addArgs(ByteString.copyFrom(serialized, 0, valueLen));
                data.reset();
            }
            builder.addBatch(args);
        }

        return builder.build();
    }

    public static Call call(String sql, List<SQLType> outParameters, Value... arguments) {
        Arguments.Builder args = Arguments.newBuilder();
        Call.Builder builder = Call.newBuilder().setSql(sql).setArguments(args);
        Data data = Data.create(NULL_HANDLER, 1024, false);
        for (Value argument : arguments) {
            int valueLen = Data.getValueLen(argument, false);
            data.checkCapacity(valueLen);
            data.writeValue(argument);
            byte[] serialized = data.getBytes();
            args.addArgs(ByteString.copyFrom(serialized, 0, valueLen));
            data.reset();
        }

        return builder.build();
    }

    public static Statement statement(String sql, Value... parameters) {
        Arguments.Builder args = Arguments.newBuilder();
        Statement.Builder builder = Statement.newBuilder().setSql(sql).setArguments(args);
        Data data = Data.create(NULL_HANDLER, 1024, false);
        for (Value parameter : parameters) {
            int valueLen = Data.getValueLen(parameter, false);
            data.checkCapacity(valueLen);
            data.writeValue(parameter);
            byte[] serialized = data.getBytes();
            args.addArgs(ByteString.copyFrom(serialized, 0, valueLen));
            data.reset();
        }

        return builder.build();
    }

    private Helper() {
    }
}
