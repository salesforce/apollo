/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common.queue;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.salesforce.apollo.snow.database.Database;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class prefixedState {

    public static final byte blockingID, jobID, stackID, stackSizeID;

    private static final byte[] stackSize;

    static {
        stackSizeID = 0;
        stackID = 1;
        jobID = 2;
        blockingID = 3;
        stackSize = new byte[] { stackID };
    }

    private final state state;

    public prefixedState(state state) {
        this.state = state;
    }

    public void addBlocking(Database db, ID id, ID blocking) {
        ByteBuffer buff = ByteBuffer.allocate(1 + ID.BYTE_SIZE);
        buff.put(blockingID);
        id.write(buff);
        state.addID(db, buff.array(), blocking);
    }

    public Collection<ID> blocking(Database db, ID id) {
        ByteBuffer buff = ByteBuffer.allocate(1 + ID.BYTE_SIZE);
        buff.put(blockingID);
        id.write(buff);
        return state.ids(db, buff.array());
    }

    public void deleteBlocking(Database db, ID id, Collection<ID> blocking) {
        ByteBuffer buff = ByteBuffer.allocate(1 + ID.BYTE_SIZE);
        buff.put(blockingID);
        id.write(buff);
        for (ID blocked : blocking) {
            state.removeID(db, buff.array(), blocked);
        }
    }

    public void deleteJob(Database db, ID id) {
        ByteBuffer buff = ByteBuffer.allocate(1 + ID.BYTE_SIZE);
        buff.put(jobID);
        id.write(buff);
        state.delete(buff.array());
    }

    public void deleteStackIndex(Database db, int index) {
        ByteBuffer buff = ByteBuffer.allocate(1 + 4);
        buff.put(stackID);
        buff.putInt(index);
        state.delete(buff.array());
    }

    public boolean hasJob(Database db, ID id) {
        ByteBuffer buff = ByteBuffer.allocate(1 + ID.BYTE_SIZE);
        buff.put(jobID);
        id.write(buff);

        return db.has(buff.array());
    }

    public Job job(Database db, ID id) {
        ByteBuffer buff = ByteBuffer.allocate(1 + ID.BYTE_SIZE);
        buff.put(jobID);
        id.write(buff);
        return state.job(db, buff.array());
    }

    public void setJob(Database db, Job job) {
        ByteBuffer buff = ByteBuffer.allocate(1 + ID.BYTE_SIZE);
        buff.put(jobID);
        job.id().write(buff);
        state.setJob(db, buff.array(), job);
    }

    public void setStackIndex(Database db, int index, Job job) {
        ByteBuffer buff = ByteBuffer.allocate(1 + 4);
        buff.put(stackID);
        buff.putInt(index);
        state.setJob(db, buff.array(), job);
    }

    public void setStackSize(Database db, int size) {
        state.setInt(db, stackSize, size);
    }

    public Job stackIndex(Database db, int index) {
        ByteBuffer buff = ByteBuffer.allocate(1 + 4);
        buff.put(stackID);
        buff.putInt(index);

        return state.job(db, buff.array());
    }

    public int stackSize(Database db) {
        return state.getInt(db, stackSize);
    }
}
