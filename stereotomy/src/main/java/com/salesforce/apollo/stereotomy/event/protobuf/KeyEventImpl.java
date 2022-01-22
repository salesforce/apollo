/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;

import org.joou.ULong;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.EventCommon;
import com.salesfoce.apollo.stereotomy.event.proto.Header;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * Grpc implemention of abstract KeyEvent
 * 
 * @author hal.hildebrand
 *
 */
abstract public class KeyEventImpl implements KeyEvent {

    private final EventCommon common;
    private final Header      header;

    public KeyEventImpl(Header header, EventCommon common) {
        this.header = header;
        this.common = common;
    }

    @Override
    public JohnHancock getAuthentication() {
        return signature(common.getAuthentication());
    }

    @Override
    public final byte[] getBytes() {
        return toByteString().toByteArray();
    }

    @Override
    public Format getFormat() {
        return Format.valueOf(header.getVersion().getFormat());
    }

    @Override
    public Identifier getIdentifier() {
        return identifier(header.getIdentifier());
    }

    @Override
    public String getIlk() {
        return header.getIlk();
    }

    @Override
    public EventCoordinates getPrevious() {
        return EventCoordinates.from(common.getPrevious());
    }

    @Override
    public Digest getPriorEventDigest() {
        return digest(header.getPriorEventDigest());
    }

    @Override
    public ULong getSequenceNumber() {
        return ULong.valueOf(header.getSequenceNumber());
    }

    @Override
    public Version getVersion() {
        return new Version() {

            @Override
            public int getMajor() {
                return header.getVersion().getMajor();
            }

            @Override
            public int getMinor() {
                return header.getVersion().getMinor();
            }
        };
    }

    @Override
    public Digest hash(DigestAlgorithm digest) {
        return new Digest(digest, digest.hashOf(toByteString()));
    }

    protected abstract ByteString toByteString();
}
