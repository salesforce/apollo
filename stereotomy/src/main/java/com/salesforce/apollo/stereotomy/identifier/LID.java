/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;

/**
 * @author hal.hildebrand
 *
 */
public class LID implements Identifier {
    private static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[] { 5 });

    private final Identifier aid;
    private final String     lid;

    public LID(ByteBuffer buff) {
        this.aid = Identifier.from(buff);
        byte[] encoded = new byte[buff.remaining()];
        buff.get(encoded);
        this.lid = new String(encoded);
    }

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
    public ByteString toByteString() {
        byte[] encoded = lid.getBytes();
        return IDENTIFIER.concat(aid.toByteString()).concat(ByteString.copyFrom(encoded));
    }

    @Override
    public String toString() {
        return "LID[" + aid + "|" + lid + "]";
    }

}
