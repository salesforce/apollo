/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import sandbox.java.lang.DJVM;
import sandbox.java.lang.Object;
import sandbox.java.sql.RowId;

/**
 * @author hal.hildebrand
 *
 */
public class RowIdWrapper implements RowId {
    private final java.sql.RowId wrapped;

    public RowIdWrapper(java.sql.RowId wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public boolean equals(Object obj) {
        return wrapped.equals(DJVM.unsandbox(obj));
    }

    @Override
    public byte[] getBytes() {
        return wrapped.getBytes();
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    public String toString() {
        return wrapped.toString();
    }

    @Override
    public java.sql.RowId toJsRowId() {
        return wrapped;
    }
}
