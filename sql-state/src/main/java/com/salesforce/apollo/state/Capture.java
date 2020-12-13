/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.util.ArrayList;
import java.util.List;

import org.h2.api.JavaObjectSerializer;
import org.h2.engine.Session.CDC;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.LobStorageInterface;
import org.h2.table.Table;
import org.h2.util.SmallLRUCache;
import org.h2.util.TempFileDeleter;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.state.proto.Delete;
import com.salesfoce.apollo.state.proto.Insert;
import com.salesfoce.apollo.state.proto.Result;
import com.salesfoce.apollo.state.proto.Results;
import com.salesfoce.apollo.state.proto.Update;
import com.salesforce.apollo.state.h2.Cdc;

/**
 * @author hal.hildebrand
 *
 */
public class Capture implements Cdc {
    public static class CdcEvent {
        public final CDC   operation;
        public final Row   prev;
        public final Table table;
        public final Row   value;

        public CdcEvent(Table table, Row prev, CDC operation, Row value) {
            this.table = table;
            this.prev = prev;
            this.operation = operation;
            this.value = value;
        }

    }

    private static class NullHandler implements DataHandler {

        @Override
        public void checkPowerOff() throws DbException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkWritingAllowed() throws DbException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompareMode getCompareMode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getDatabasePath() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JavaObjectSerializer getJavaObjectSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getLobCompressionAlgorithm(int type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SmallLRUCache<String, String[]> getLobFileListCache() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LobStorageInterface getLobStorage() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getLobSyncObject() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMaxLengthInplaceLob() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TempFileDeleter getTempFileDeleter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStore openFile(String name, String mode, boolean mustExist) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
            throw new UnsupportedOperationException();
        }
    }

    private static final NullHandler NULL = new NullHandler();

    private final List<CdcEvent> changes = new ArrayList<>();

    @Override
    public void cdc(Table table, Row prev, CDC operation, Row value) {
        changes.add(new CdcEvent(table, prev, operation, value));
    }

    public List<CdcEvent> getChanges() {
        return changes;
    }

    public Results results() {
        Results.Builder builder = Results.newBuilder();
        changes.forEach(event -> builder.addResults(resultOf(event)));
        return builder.build();
    }

    private Result resultOf(CdcEvent event) {

        switch (event.operation) {
        case DELETE:
            return Result.newBuilder()
                         .setDelete(Delete.newBuilder().setTable(event.table.getId()).setKey(event.prev.getKey()))
                         .build();
        case INSERT: {
            Value[] values = event.value.getValueList();
            Data buff = Data.create(NULL, 0, true);
            int length = 0;
            for (Value v : values) {
                length += buff.getValueLen(v);
            }
            buff = Data.create(NULL, length, false);
            for (Value value : values) {
                buff.writeValue(value);
            }
            return Result.newBuilder()
                         .setInsert(Insert.newBuilder()
                                          .setTable(event.table.getId())
                                          .setKey(event.value.getKey())
                                          .setCount(values.length)
                                          .setValues(ByteString.copyFrom(buff.getBytes())))
                         .build();
        }
        case UPDATE: {
            List<Integer> mappings = new ArrayList<>();
            List<Value> updateValues = new ArrayList<>();
            Value[] previous = event.prev.getValueList();
            Value[] updates = event.value.getValueList();
            Data buff = Data.create(NULL, 0, true);
            int length = 0;
            for (int i = 0; i < updates.length; i++) {
                Value v = updates[i];
                Value p = previous[i];
                if (!v.equals(p)) {
                    mappings.add(i);
                    updateValues.add(v);
                    length += buff.getValueLen(v);
                }
            }
            buff = Data.create(NULL, length, false);
            for (Value value : updateValues) {
                buff.writeValue(value);
            }
            return Result.newBuilder()
                         .setUpdate(Update.newBuilder()
                                          .setTable(event.table.getId())
                                          .setKey(event.value.getKey())
                                          .setCount(updateValues.size())
                                          .setValues(ByteString.copyFrom(buff.getBytes())))
                         .build();
        }
        default:
            throw new IllegalStateException("unknown operation: " + event.operation);

        }
    }
}
