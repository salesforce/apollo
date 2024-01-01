/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import com.google.protobuf.Any;
import com.salesforce.apollo.stereotomy.event.proto.EventLoc;
import com.salesforce.apollo.stereotomy.event.proto.Sealed;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 */
public interface Seal {

    static Seal from(Sealed s) {
        if (s.hasEventCoordinates()) {
            return construct(new EventCoordinates(s.getEventCoordinates()));
        }
        if (s.hasDigest()) {
            return DigestSeal.construct(Digest.from(s.getDigest()));
        }
        if (s.hasEvent()) {
            EventLoc event = s.getEvent();
            return EventSeal.construct(Identifier.from(event.getIdentifier()), new Digest(event.getDigest()),
                                       event.getSequenceNumber());
        }
        return ProtoSeal.construct(s.getValue());
    }

    static CoordinatesSeal construct(EventCoordinates coordinates) {
        return new CoordinatesSealImpl(coordinates);
    }

    Sealed toSealed();

    Any value();

    interface CoordinatesSeal extends Seal {

        EventCoordinates getEvent();
    }

    interface DigestSeal extends Seal {

        static DigestSeal construct(Digest digest) {
            return new DigestSealImpl(digest);
        }

        Digest getDigest();

        class DigestSealImpl extends ProtoSeal implements DigestSeal {

            private final Digest digest;

            public DigestSealImpl(Digest digest) {
                this(digest, null);
            }

            public DigestSealImpl(Digest digest, Any value) {
                super(value);
                this.digest = digest;
            }

            @Override
            public Digest getDigest() {
                return digest;
            }

            @Override
            public Sealed toSealed() {
                return Sealed.newBuilder().setDigest(digest.toDigeste()).setValue(value()).build();
            }
        }
    }

    interface EventSeal extends Seal {

        static EventSeal construct(Identifier prefix, Digest digest, long sequenceNumber) {
            return new EventSealImpl(prefix, digest, sequenceNumber);
        }

        Digest getDigest();

        Identifier getPrefix();

        long getSequenceNumber();

        class EventSealImpl extends ProtoSeal implements EventSeal {
            private final Identifier prefix;
            private final Digest     digest;
            private final long       sequenceNumber;

            public EventSealImpl(Identifier prefix, Digest digest, long sequenceNumber) {
                this(prefix, digest, sequenceNumber, null);
            }

            public EventSealImpl(Identifier prefix, Digest digest, long sequenceNumber, Any value) {
                super(value);
                this.prefix = prefix;
                this.digest = digest;
                this.sequenceNumber = sequenceNumber;
            }

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
            public Sealed toSealed() {
                return Sealed.newBuilder()
                             .setEvent(EventLoc.newBuilder()
                                               .setDigest(digest.toDigeste())
                                               .setIdentifier(prefix.toIdent())
                                               .setSequenceNumber(sequenceNumber))
                             .setValue(value())
                             .build();
            }
        }
    }

    class ProtoSeal implements Seal {
        private final Any value;

        public ProtoSeal(Any value) {
            this.value = value == null ? Any.getDefaultInstance() : value;
        }

        static ProtoSeal construct(Any value) {
            return new Seal.ProtoSeal(value);
        }

        @Override
        public Sealed toSealed() {
            return Sealed.newBuilder().setValue(value).build();
        }

        @Override
        public Any value() {
            return value;
        }
    }

    class CoordinatesSealImpl extends ProtoSeal implements CoordinatesSeal {
        private final EventCoordinates coordinates;

        public CoordinatesSealImpl(EventCoordinates coordinates) {
            this(coordinates, null);
        }

        public CoordinatesSealImpl(EventCoordinates coordinates, Any value) {
            super(value);
            this.coordinates = coordinates;
        }

        @Override
        public EventCoordinates getEvent() {
            return coordinates;
        }

        @Override
        public Sealed toSealed() {
            return Sealed.newBuilder().setEventCoordinates(coordinates.toEventCoords()).build();
        }
    }
}
