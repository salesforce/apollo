/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import com.google.protobuf.Message;
import com.salesfoce.apollo.state.proto.Batch;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesfoce.apollo.state.proto.BatchedTransaction;
import com.salesfoce.apollo.state.proto.Call;
import com.salesfoce.apollo.state.proto.Script;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.consortium.Consortium;
import com.salesforce.apollo.protocols.HashKey;
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
            @SuppressWarnings("unchecked")
            public BatchBuilder andThen(BiConsumer<Result, Throwable> processor) {
                completions.add((CompletableFuture<Object>) new CompletableFuture<Result>().whenComplete(processor));
                return BatchBuilder.this;
            }

            public BatchBuilder discard() {
                completions.add(null);
                return BatchBuilder.this;
            }
        }

        private final BatchedTransaction.Builder           batch       = BatchedTransaction.newBuilder();
        private final ArrayList<CompletableFuture<Object>> completions = new ArrayList<>();
        private final Consortium                           node;

        public BatchBuilder(Consortium node) {
            this.node = node;
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

        public HashKey submit() {
            return node.submit(null, (r, t) -> process(r, t), build());
        }

        public HashKey submit(BiConsumer<Boolean, Throwable> onSubmit) {
            return node.submit(onSubmit, (r, t) -> process(r, t), build());
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

    private final Consortium node;

    public Mutator(Consortium node) {
        this.node = node;
    }

    public BatchBuilder batch() {
        return new BatchBuilder(node);
    }

    public HashKey execute(Batch batch) {
        return node.submit(null, null, batch);
    }

    public HashKey execute(Batch batch, BiConsumer<int[], Throwable> processor) {
        return node.submit(null, processor, batch);
    }

    public HashKey execute(Batch batch, BiConsumer<int[], Throwable> processor,
                           BiConsumer<Boolean, Throwable> onSubmit) {
        return node.submit(onSubmit, processor, batch);
    }

    public HashKey execute(BatchUpdate batchUpdate) {
        return node.submit(null, null, batchUpdate);
    }

    public HashKey execute(BatchUpdate batchUpdate, BiConsumer<List<ResultSet>, Throwable> processor) {
        return node.submit(null, processor, batchUpdate);
    }

    public HashKey execute(BatchUpdate batchUpdate, BiConsumer<List<ResultSet>, Throwable> processor,
                           BiConsumer<Boolean, Throwable> onSubmit) {
        return node.submit(onSubmit, processor, batchUpdate);
    }

    public <T> HashKey execute(Call call) {
        return node.submit(null, null, call);
    }

    public HashKey execute(Call call, BiConsumer<CallResult, Throwable> processor) {
        return node.submit(null, processor, call);
    }

    public HashKey execute(Call call, BiConsumer<CallResult, Throwable> processor,
                           BiConsumer<Boolean, Throwable> onSubmit) {
        return node.submit(onSubmit, processor, call);
    }

    public HashKey execute(Script script) {
        return node.submit(null, null, script);
    }

    public <T> HashKey execute(Script script, BiConsumer<T, Throwable> processor) {
        return node.submit(null, processor, script);
    }

    public <T> HashKey execute(Script script, BiConsumer<T, Throwable> processor,
                               BiConsumer<Boolean, Throwable> onSubmit) {
        return node.submit(onSubmit, processor, script);
    }

    public HashKey execute(Statement statement) {
        return node.submit(null, null, statement);
    }

    public HashKey execute(Statement statement, BiConsumer<List<ResultSet>, Throwable> processor) {
        return node.submit(null, processor, statement);
    }

    public HashKey execute(Statement statement, BiConsumer<List<ResultSet>, Throwable> processor,
                           BiConsumer<Boolean, Throwable> onSubmit) {
        return node.submit(onSubmit, processor, statement);
    }
}
