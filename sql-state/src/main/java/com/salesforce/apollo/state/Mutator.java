/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.h2.api.Interval;
import org.h2.api.TimestampWithTimeZone;
import org.h2.message.DbException;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.util.JSR310;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueInterval;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringFixed;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueUuid;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.state.proto.Arguments;
import com.salesfoce.apollo.state.proto.Batch;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesfoce.apollo.state.proto.BatchedTransaction;
import com.salesfoce.apollo.state.proto.Call;
import com.salesfoce.apollo.state.proto.EXECUTION;
import com.salesfoce.apollo.state.proto.Script;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.Session;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.state.SqlStateMachine.CallResult;

/**
 * The mutation API for the materialized view
 * 
 * @author hal.hildebrand
 * 
 */
public class Mutator {
    public static class BatchBuilder {

        public class Completion<Result> {
            public BatchBuilder andThen(@SuppressWarnings("rawtypes") CompletableFuture processor) {
                completions.add(processor);
                return BatchBuilder.this;
            }

            public BatchBuilder discard() {
                completions.add(null);
                return BatchBuilder.this;
            }
        }

        private final BatchedTransaction.Builder   batch       = BatchedTransaction.newBuilder();
        @SuppressWarnings("rawtypes")
        private final ArrayList<CompletableFuture> completions = new ArrayList<>();
        private final Session                      session;

        public BatchBuilder(Session session) {
            this.session = session;
        }

        public Completion<int[]> execute(BatchUpdate update) {
            batch.addTransactions(Txn.newBuilder().setBatchUpdate(update).build());
            return new Completion<>();
        }

        public Completion<CallResult> execute(Call call) {
            batch.addTransactions(Txn.newBuilder().setCall(call).build());
            return new Completion<>();
        }

        public <T> Completion<T> execute(Script script) {
            batch.addTransactions(Txn.newBuilder().setScript(script).build());
            return new Completion<>();
        }

        public Completion<List<ResultSet>> execute(Statement statement) {
            batch.addTransactions(Txn.newBuilder().setStatement(statement).build());
            return new Completion<>();
        }

        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<T> submit(Executor exec, Duration timeout,
                                               ScheduledExecutorService scheduler) throws InvalidTransaction {
            return (CompletableFuture<T>) session.submit(exec, build(), timeout, scheduler)
                                                 .whenComplete((BiConsumer<Object, Throwable>) (r, t) -> process(r, t));
        }

        private Message build() {
            return batch.build();
        }

        private void process(Object r, Throwable t) {
            if (t instanceof BatchedTransactionException) {
                BatchedTransactionException e = (BatchedTransactionException) t;
                completions.get(e.getIndex()).completeExceptionally(e.getCause());
                return;
            }
            @SuppressWarnings("unchecked")
            List<Object> results = (List<Object>) r;
            assert results.size() == completions.size() : "Results: " + results.size() + " does not match Completions: "
            + completions.size();
            for (int i = 0; i < results.size(); i++) {
                @SuppressWarnings("unchecked")
                CompletableFuture<Object> futureSailor = completions.get(i);
                if (futureSailor != null) {
                    futureSailor.complete(results.get(i));
                }
            }
        }
    }

    public static class BatchedTransactionException extends Exception {

        private static final long serialVersionUID = 1L;

        private final int index;

        public BatchedTransactionException(int index, String message, Throwable cause) {
            super(message, cause);
            this.index = index;
        }

        public BatchedTransactionException(int index, Throwable cause) {
            this(index, null, cause);
        }

        public int getIndex() {
            return index;
        }

    }

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
                args.addArgs(serialized(data, argument));
            }
            builder.addBatch(args);
        }

        return builder.build();
    }

    public static BatchUpdate batchOf(String sql, List<List<Object>> batch) {
        return batch(sql, batch.stream().map(args -> args.stream().map(o -> convert(o)).collect(Collectors.toList()))
                               .collect(Collectors.toList()));
    }

    public static Call call(EXECUTION execution, String sql, List<SQLType> outParameters, Object... arguments) {
        Value[] argValues = new Value[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            argValues[i] = convert(arguments[i]);
        }
        return call(execution, sql, outParameters, argValues);
    }

    public static Call call(EXECUTION execution, String sql, List<SQLType> outParameters, Value... arguments) {
        Call.Builder builder = Call.newBuilder().setSql(sql);
        Data data = Data.create(NULL_HANDLER, 1024, false);
        for (Value argument : arguments) {
            builder.addArgs(serialized(data, argument));
        }

        return builder.build();
    }

    public static Call call(String sql, List<SQLType> outParameters, Object... arguments) {
        return call(EXECUTION.EXECUTE, sql, outParameters, arguments);
    }

    public static Script callScript(String className, String method, String source, Value... args) {
        Data data = Data.create(NULL_HANDLER, 1024, false);
        return Script.newBuilder().setClassName(className).setMethod(method).setSource(source)
                     .addAllArgs(Arrays.asList(args).stream().map(v -> serialized(data, v))
                                       .collect(Collectors.toList()))
                     .build();
    }

    public static Value convert(Object x) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (x instanceof String) {
            return ValueString.get((String) x);
        } else if (x instanceof Value) {
            return (Value) x;
        } else if (x instanceof Long) {
            return ValueLong.get((Long) x);
        } else if (x instanceof Integer) {
            return ValueInt.get((Integer) x);
        } else if (x instanceof BigInteger) {
            return ValueDecimal.get((BigInteger) x);
        } else if (x instanceof BigDecimal) {
            return ValueDecimal.get((BigDecimal) x);
        } else if (x instanceof Boolean) {
            return ValueBoolean.get((Boolean) x);
        } else if (x instanceof Byte) {
            return ValueByte.get((Byte) x);
        } else if (x instanceof Short) {
            return ValueShort.get((Short) x);
        } else if (x instanceof Float) {
            return ValueFloat.get((Float) x);
        } else if (x instanceof Double) {
            return ValueDouble.get((Double) x);
        } else if (x instanceof byte[]) {
            return ValueBytes.get((byte[]) x);
        } else if (x instanceof Date) {
            return ValueDate.get(null, (Date) x);
        } else if (x instanceof Time) {
            return ValueTime.get(null, (Time) x);
        } else if (x instanceof Timestamp) {
            return ValueTimestamp.get(null, (Timestamp) x);
        } else if (x instanceof java.util.Date) {
            return ValueTimestamp.fromMillis(((java.util.Date) x).getTime(), 0);
        } else if (x instanceof java.sql.Array) {
            java.sql.Array array = (java.sql.Array) x;
            try {
                return convert(array.getArray());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof UUID) {
            return ValueUuid.get((UUID) x);
        }
        Class<?> clazz = x.getClass();
        if (x instanceof Object[]) {
            // (a.getClass().isArray());
            // (a.getClass().getComponentType().isPrimitive());
            Object[] o = (Object[]) x;
            int len = o.length;
            Value[] v = new Value[len];
            for (int i = 0; i < len; i++) {
                v[i] = convert(o[i]);
            }
            return ValueArray.get(clazz.getComponentType(), v);
        } else if (x instanceof Character) {
            return ValueStringFixed.get(((Character) x).toString());
        } else if (clazz == JSR310.LOCAL_DATE) {
            return JSR310Utils.localDateToValue(x);
        } else if (clazz == JSR310.LOCAL_TIME) {
            return JSR310Utils.localTimeToValue(x);
        } else if (clazz == JSR310.LOCAL_DATE_TIME) {
            return JSR310Utils.localDateTimeToValue(x);
        } else if (clazz == JSR310.INSTANT) {
            return JSR310Utils.instantToValue(x);
        } else if (clazz == JSR310.OFFSET_TIME) {
            return JSR310Utils.offsetTimeToValue(x);
        } else if (clazz == JSR310.OFFSET_DATE_TIME) {
            return JSR310Utils.offsetDateTimeToValue(x);
        } else if (clazz == JSR310.ZONED_DATE_TIME) {
            return JSR310Utils.zonedDateTimeToValue(x);
        } else if (x instanceof TimestampWithTimeZone) {
            return ValueTimestampTimeZone.get((TimestampWithTimeZone) x);
        } else if (x instanceof Interval) {
            Interval i = (Interval) x;
            return ValueInterval.from(i.getQualifier(), i.isNegative(), i.getLeading(), i.getRemaining());
        } else if (clazz == JSR310.PERIOD) {
            return JSR310Utils.periodToValue(x);
        } else if (clazz == JSR310.DURATION) {
            return JSR310Utils.durationToValue(x);
        } else {
            throw new IllegalArgumentException("Unknown value type: " + x.getClass());
        }

    }

    public static ByteString serialized(Data data, Value arg) {
        int valueLen = Data.getValueLen(arg, false);
        data.checkCapacity(valueLen);
        data.writeValue(arg);
        byte[] serialized = data.getBytes();
        ByteString bs = ByteString.copyFrom(serialized, 0, valueLen);
        data.reset();
        return bs;
    }

    public static Statement statement(EXECUTION execution, String sql, Object... args) {
        Value[] paramValues = new Value[args.length];
        for (int i = 0; i < args.length; i++) {
            paramValues[i] = convert(args[i]);
        }

        return statement(execution, sql, paramValues);
    }

    public static Statement statement(EXECUTION execution, String sql, Value... args) {
        Statement.Builder builder = Statement.newBuilder().setSql(sql);
        Data data = Data.create(NULL_HANDLER, 1024, false);
        for (Value arg : args) {
            builder.addArgs(serialized(data, arg));
        }

        return builder.build();
    }

    public static Statement statement(String sql, Object... args) {
        return statement(EXECUTION.EXECUTE, sql, args);
    }

    private final Session session;

    public Mutator(Session node) {
        this.session = node;
    }

    public BatchBuilder batch() {
        return new BatchBuilder(session);
    }

    public CompletableFuture<int[]> execute(Executor exec, Batch batch, Duration timeout,
                                            ScheduledExecutorService scheduler) throws InvalidTransaction {
        return session.submit(exec, Txn.newBuilder().setBatch(batch).build(), timeout, scheduler);
    }

    public CompletableFuture<int[]> execute(Executor exec, BatchUpdate batchUpdate, Duration timeout,
                                            ScheduledExecutorService scheduler) throws InvalidTransaction {
        return session.submit(exec, Txn.newBuilder().setBatchUpdate(batchUpdate).build(), timeout, scheduler);
    }

    public CompletableFuture<CallResult> execute(Executor exec, Call call, Duration timeout,
                                                 ScheduledExecutorService scheduler) throws InvalidTransaction {
        return session.submit(exec, Txn.newBuilder().setCall(call).build(), timeout, scheduler);
    }

    public <T> CompletableFuture<T> execute(Executor exec, Script script, Duration timeout,
                                            ScheduledExecutorService scheduler) throws InvalidTransaction {
        return session.submit(exec, Txn.newBuilder().setScript(script).build(), timeout, scheduler);
    }

    public CompletableFuture<List<ResultSet>> execute(Executor exec, Statement statement, Duration timeout,
                                                      ScheduledExecutorService scheduler) throws InvalidTransaction {
        return session.submit(exec, Txn.newBuilder().setStatement(statement).build(), timeout, scheduler);
    }
}
