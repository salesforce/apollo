/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.AID;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;

/**
 * @author hal.hildebrand
 *
 */
public class AutonomicIdentifier implements Identifier {
    private static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[] { 4 });

    private final Identifier prefix;
    private final URI        uri;

    public AutonomicIdentifier(ByteBuffer buff) {
        this.prefix = Identifier.from(buff);
        byte[] encoded = new byte[buff.remaining()];
        buff.get(encoded);
        this.uri = URI.create(new String(encoded));
    }

    public AutonomicIdentifier(Identifier prefix, URI uri) {
        this.prefix = prefix;
        this.uri = uri;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AutonomicIdentifier)) {
            return false;
        }
        AutonomicIdentifier other = (AutonomicIdentifier) obj;
        return Objects.equals(prefix, other.prefix) && Objects.equals(uri, other.uri);
    }

    public Identifier getPrefix() {
        return prefix;
    }

    public URI getUri() {
        return uri;
    }

    @Override
    public int hashCode() {
        return Objects.hash(prefix, uri);
    }

    @Override
    public byte identifierCode() {
        return 4;
    }

    @Override
    public boolean isTransferable() {
        return prefix.isTransferable();
    }

    @Override
    public ByteString toByteString() {
        byte[] encoded = uri.toASCIIString().getBytes();
        return IDENTIFIER.concat(prefix.toByteString()).concat(ByteString.copyFrom(encoded));
    }

    @Override
    public Ident toIdent() {
        return Ident.newBuilder()
                    .setAutonomous(AID.newBuilder().setPrefix(prefix.toIdent()).setUrl(uri.toASCIIString()))
                    .build();
    }

    @Override
    public String toString() {
        return "AID[" + prefix + ":" + uri + "]";
    }

}
