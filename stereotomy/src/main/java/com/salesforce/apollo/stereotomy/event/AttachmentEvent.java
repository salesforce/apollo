/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author hal.hildebrand
 */
public interface AttachmentEvent {
    static Attachment of(Seal... seals) {
        return new AttachmentImpl(Arrays.asList(seals));
    }

    Attachment attachments();

    EventCoordinates coordinates();

    byte[] getBytes();

    com.salesforce.apollo.stereotomy.event.proto.AttachmentEvent toEvent_();

    Version version();

    interface Attachment {

        Attachment EMPTY = new Attachment() {

            @Override
            public Map<Integer, JohnHancock> endorsements() {
                return Collections.emptyMap();
            }

            @Override
            public List<Seal> seals() {
                return Collections.emptyList();
            }
        };

        static Attachment of(com.salesforce.apollo.stereotomy.event.proto.Attachment attachment) {
            return new AttachmentImpl(attachment.getSealsList().stream().map(Seal::from).toList(),
                                      attachment.getEndorsementsMap()
                                                .entrySet()
                                                .stream()
                                                .collect(Collectors.toMap(Map.Entry::getKey,
                                                                          e -> JohnHancock.of(e.getValue()))));
        }

        Map<Integer, JohnHancock> endorsements();

        List<Seal> seals();

        default com.salesforce.apollo.stereotomy.event.proto.Attachment toAttachemente() {
            var builder = com.salesforce.apollo.stereotomy.event.proto.Attachment.newBuilder();
            builder.addAllSeals(seals().stream().map(Seal::toSealed).toList())
                   .putAllEndorsements(endorsements().entrySet()
                                                     .stream()
                                                     .collect(
                                                     Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toSig())));
            return builder.build();
        }
    }

    class AttachmentImpl implements Attachment {
        private final Map<Integer, JohnHancock> endorsements;
        private final List<Seal>                seals;

        public AttachmentImpl(List<Seal> seals) {
            this.seals = seals;
            this.endorsements = Collections.emptyMap();
        }

        public AttachmentImpl(List<Seal> seals, Map<Integer, JohnHancock> endorsements) {
            this.seals = seals;
            this.endorsements = new TreeMap<>(endorsements);
        }

        public AttachmentImpl(Map<Integer, JohnHancock> endorsements) {
            this.seals = Collections.emptyList();
            this.endorsements = new TreeMap<>(endorsements);
        }

        public AttachmentImpl(Seal... seals) {
            this.seals = Arrays.asList(seals);
            this.endorsements = Collections.emptyMap();
        }

        @Override
        public Map<Integer, JohnHancock> endorsements() {
            return endorsements;
        }

        @Override
        public List<Seal> seals() {
            return seals;
        }

    }
}
