/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import java.util.Objects;

import com.salesfoce.apollo.stereotomy.event.proto.Ident;

/**
 * @author hal.hildebrand
 *
 */
public class LID implements Identifier {

    private final Identifier aid;
    private final String     lid;

    public LID(Identifier aid, String lid) {
        this.aid = aid;
        this.lid = lid;
    }

    @Override
    public Ident toIdent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LID)) {
            return false;
        }
        LID other = (LID) obj;
        return Objects.equals(aid, other.aid) && Objects.equals(lid, other.lid);
    }

    public Identifier getAid() {
        return aid;
    }

    public String getLid() {
        return lid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(aid, lid);
    }

    @Override
    public byte identifierCode() {
        return 5;
    }

    @Override
    public boolean isTransferable() {
        return aid.isTransferable();
    }

    @Override
    public String toString() {
        return "LID[" + aid + "|" + lid + "]";
    }

}
