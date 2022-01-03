/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import com.salesfoce.apollo.stereotomy.event.proto.EventLoc;
import com.salesfoce.apollo.stereotomy.event.proto.Sealed;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface Seal {
    interface CoordinatesSeal extends Seal {

        static CoordinatesSeal construct(EventCoordinates coordinates) {
            return new CoordinatesSeal() {

                @Override
                public EventCoordinates getEvent() {
                    return coordinates;
                }

                @Override
                public Sealed toSealed() {
                    return Sealed.newBuilder().setEventCoordinates(coordinates.toEventCoords()).build();
                }
            };
        }

        EventCoordinates getEvent();
    }

    interface DelegatingLocationSeal extends Seal {

        static DelegatingLocationSeal construct(DelegatingEventCoordinates coordinates) {
            return new DelegatingLocationSeal() {

                @Override
                public DelegatingEventCoordinates getCoordinates() {
                    return coordinates;
                }

                @Override
                public Sealed toSealed() {
                    return Sealed.newBuilder().setDelegatingLocation(coordinates.toCoords()).build();
                }
            };
        }

        DelegatingEventCoordinates getCoordinates();

    }

    interface DigestSeal extends Seal {

        static DigestSeal construct(Digest digest) {
            return new DigestSeal() {

                @Override
                public Digest getDigest() {
                    return digest;
                }

                @Override
                public Sealed toSealed() {
                    return Sealed.newBuilder().setDigest(digest.toDigeste()).build();
                }
            };
        }

        Digest getDigest();
    }

    interface EventSeal extends Seal {

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
                public Sealed toSealed() {
                    return Sealed.newBuilder()
                                 .setEvent(EventLoc.newBuilder()
                                                   .setIdentifier(prefix.toIdent())
                                                   .setSequenceNumber(sequenceNumber))
                                 .setDigest(digest.toDigeste())
                                 .build();
                }
            };
        }

        Digest getDigest();

        Identifier getPrefix();

        long getSequenceNumber();
    }

    static Seal from(Sealed s) {
        if (s.hasEventCoordinates()) {
            return CoordinatesSeal.construct(new EventCoordinates(s.getEventCoordinates()));
        }
        if (s.hasDelegatingLocation()) {
            return DelegatingLocationSeal.construct(new DelegatingEventCoordinates(s.getDelegatingLocation()));
        }
        if (s.hasDigest()) {
            return DigestSeal.construct(Digest.from(s.getDigest()));
        }
        if (s.hasEvent()) {
            EventLoc event = s.getEvent();
            return EventSeal.construct(Identifier.from(event.getIdentifier()), new Digest(event.getDigest()),
                                       event.getSequenceNumber());
        }
        throw new IllegalArgumentException("Unknown seal type");
    }

    Sealed toSealed();
}
