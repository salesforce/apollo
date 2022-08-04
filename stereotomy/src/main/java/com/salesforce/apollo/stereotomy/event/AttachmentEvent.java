/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;

/**
 * @author hal.hildebrand
 *
 */
public interface AttachmentEvent {
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

        static Attachment of(com.salesfoce.apollo.stereotomy.event.proto.Attachment attachment) {
            return new AttachmentImpl(attachment.getSealsList().stream().map(s -> Seal.from(s)).toList(),
                                      attachment.getEndorsementsMap()
                                                .entrySet()
                                                .stream()
                                                .collect(Collectors.toMap(e -> e.getKey(),
                                                                          e -> JohnHancock.of(e.getValue()))));
        }

        Map<Integer, JohnHancock> endorsements();

        List<Seal> seals();

        default com.salesfoce.apollo.stereotomy.event.proto.Attachment toAttachemente() {
            var builder = com.salesfoce.apollo.stereotomy.event.proto.Attachment.newBuilder();
            builder.addAllSeals(seals().stream().map(s -> s.toSealed()).toList())
                   .putAllEndorsements(endorsements().entrySet()
                                                     .stream()
                                                     .collect(Collectors.toMap(e -> e.getKey(),
                                                                               e -> e.getValue().toSig())));
            return builder.build();
        }
    }

    static class AttachmentImpl implements Attachment {
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

    static Attachment of(Seal... seals) {
        return new AttachmentImpl(Arrays.asList(seals));
    }

    Attachment attachments();

    EventCoordinates coordinates();

    byte[] getBytes();

    com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent toEvent_();

    Version version();
}
