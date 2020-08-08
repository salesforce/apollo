/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.h2.api.JavaObjectSerializer;
import org.h2.engine.Constants;
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

import com.salesforce.apollo.state.h2.Cdc;

/**
 * @author hal.hildebrand
 *
 */
public class Transaction implements Cdc {

    public static class Statement {
        public final List<SqlIdentifier> arguments = new ArrayList<>();
        public final SqlNode             query;

        public Statement(SqlNode query) {
            this.query = query;
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

    private static final NullHandler NULL       = new NullHandler();
    private final List<CdcEvent>     changes    = new ArrayList<>();
    private final List<Statement>    statements = new ArrayList<>();

    public List<CdcEvent> getChanges() {
        return changes;
    }

    public List<Statement> getStatements() {
        return statements;
    }

    @Override
    public void log(Table table, short operation, Row row) {
        changes.add(new CdcEvent(table, operation, row));
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzos = new GZIPOutputStream(baos);
        ByteBuffer len = ByteBuffer.allocate(4);
        len.putInt(changes.size());
        gzos.write(len.array(), 0, len.array().length);

        Data buff = Data.create(NULL, Constants.DEFAULT_PAGE_SIZE, true);
        for (int i = 0; i < changes.size(); i++) {
            CdcEvent r = changes.get(i);
            buff.checkCapacity(Constants.DEFAULT_PAGE_SIZE);
            r.append(buff);
            if (i == changes.size() - 1 || buff.length() > Constants.UNDO_BLOCK_SIZE) {
                gzos.write(buff.getBytes(), 0, buff.length());
                buff.reset();
            }
        }
        gzos.finish();
        return baos.toByteArray();
    }
}
