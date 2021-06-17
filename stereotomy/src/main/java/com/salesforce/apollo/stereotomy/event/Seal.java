/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface Seal {
    interface CoordinatesSeal extends Seal {
        static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[] { 1 });

        static CoordinatesSeal construct(EventCoordinates coordinates) {
            return new CoordinatesSeal() {

                @Override
                public EventCoordinates getEvent() {
                    return coordinates;
                }

                @Override
                public byte sealCode() {
                    return 1;
                }

                @Override
                public ByteString toByteSring() {
                    return IDENTIFIER.concat(coordinates.toByteString());
                }
            };
        }

        EventCoordinates getEvent();
    }

    interface DelegatingLocationSeal extends Seal {
        static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[] { 2 });

        static DelegatingLocationSeal construct(DelegatingEventCoordinates coordinates) {
            return new DelegatingLocationSeal() {

                @Override
                public DelegatingEventCoordinates getCoordinates() {
                    return coordinates;
                }

                @Override
                public byte sealCode() {
                    return 2;
                }

                @Override
                public ByteString toByteSring() {
                    return IDENTIFIER.concat(coordinates.toByteString());
                }
            };
        }

        DelegatingEventCoordinates getCoordinates();

    }

    interface DigestSeal extends Seal {
        static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[] { 3 });

        static DigestSeal construct(Digest digest) {
            return new DigestSeal() {

                @Override
                public Digest getDigest() {
                    return digest;
                }

                @Override
                public byte sealCode() {
                    return 3;
                }

                @Override
                public ByteString toByteSring() {
                    return IDENTIFIER.concat(digest.toByteString());
                }
            };
        }

        Digest getDigest();
    }

    interface EventSeal extends Seal {
        static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[] { 4 });

        static EventSeal construct(Identifier prefix, Digest digest, long sequenceNumber) {
            return new EventSeal() {

                @Override
                public Digest getDigest() {
                    return digest;
                }

                @Override
                public Identifier getPrefix() {
                    return prefix;
                }

                @Override
                public long getSequenceNumber() {
                    return sequenceNumber;
                }

                @Override
                public byte sealCode() {
                    return 4;
                }

                @Override
                public ByteString toByteSring() {
                    return IDENTIFIER.concat(digest.toByteString());
                }
            };
        }

        Digest getDigest();

        Identifier getPrefix();

        long getSequenceNumber();
    }

    static <T extends Seal> T from(ByteBuffer buff) {
        byte code = buff.get();
        @SuppressWarnings("unchecked")
        T seal = (T) switch (code) {
        case 1 -> CoordinatesSeal.construct(EventCoordinates.from(buff));
        case 2 -> DelegatingLocationSeal.construct(DelegatingEventCoordinates.from(buff));
        case 3 -> DigestSeal.construct(Digest.from(buff));
        case 4 -> EventSeal.construct(Identifier.from(buff), Digest.from(buff), buff.getLong());
        default -> throw new IllegalArgumentException("Unknown seal type: " + code);
        };
        return seal;
    }

    static <T extends Seal> T from(ByteString bs) {
        return from(bs.asReadOnlyByteBuffer());
    }

    byte sealCode();

    ByteString toByteSring();
}
