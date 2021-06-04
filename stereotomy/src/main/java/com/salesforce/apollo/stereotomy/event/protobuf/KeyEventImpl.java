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

import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.Header;
import com.salesfoce.apollo.stereotomy.event.proto.Receipt;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.utils.Pair;

/**
 * Grpc implemention of abstract KeyEvent
 * 
 * @author hal.hildebrand
 *
 */
abstract public class KeyEventImpl implements KeyEvent {

    public static Seal sealOf(com.salesfoce.apollo.stereotomy.event.proto.Seal s) {
        if (s.hasCoordinates()) {
            com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates coordinates = s.getCoordinates();
            return new Seal.CoordinatesSeal() {

                @Override
                public EventCoordinates getEvent() {
                    return new EventCoordinates(identifier(coordinates.getIdentifier()),
                            coordinates.getSequenceNumber(), digest(coordinates.getDigest()));
                }
            };
        }

        return new Seal.DigestSeal() {
            @Override
            public Digest getDigest() {
                return digest(s.getDigest());
            }
        };
    }

    private static Map<Integer, JohnHancock> signaturesOf(Receipt receipt) {
        return receipt.getSignaturesMap()
                      .entrySet()
                      .stream()
                      .collect(Collectors.toMap(e -> e.getKey(), e -> signature(e.getValue())));
    }

    private final Header header;

    public KeyEventImpl(Header header) {
        this.header = header;
    }

    @Override
    public Map<Integer, JohnHancock> getAuthentication() {
        return header.getAuthenticationMap()
                     .entrySet()
                     .stream()
                     .collect(Collectors.toMap(e -> e.getKey(), e -> signature(e.getValue())));
    }

    @Override
    public EventCoordinates getCoordinates() {
        com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates coordinates = header.getCoordinates();
        return new EventCoordinates(identifier(coordinates.getIdentifier()), getSequenceNumber(),
                digest(coordinates.getDigest()));
    }

    @Override
    public Map<Integer, JohnHancock> getEndorsements() {
        return header.getEndorsementsMap()
                     .entrySet()
                     .stream()
                     .collect(Collectors.toMap(e -> e.getKey(), e -> signature(e.getValue())));
    }

    @Override
    public Format getFormat() {
        return Format.valueOf(header.getFormat());
    }

    @Override
    public EventCoordinates getPrevious() {
        com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates previous = header.getPrevious();
        return new EventCoordinates(identifier(previous.getIdentifier()), previous.getSequenceNumber(),
                digest(previous.getDigest()));
    }

    @Override
    public Map<EventCoordinates, Map<Integer, JohnHancock>> getReceipts() {
        return header.getReceiptsList().stream().map(receipt -> {
            com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates coordinates = receipt.getCoordinates();
            return new Pair<EventCoordinates, Map<Integer, JohnHancock>>(
                    new EventCoordinates(identifier(coordinates.getIdentifier()), coordinates.getSequenceNumber(),
                            digest(coordinates.getDigest())),
                    signaturesOf(receipt));
        }).collect(Collectors.toMap(e -> e.a, e -> e.b));
    }

    @Override
    public Version getVersion() {
        return new Version() {

            @Override
            public int major() {
                return header.getVersion().getMajor();
            }

            @Override
            public int minor() {
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
