/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.KeyStateWithEndorsementsAndValidations;
import com.salesforce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The Key Event Receipt Log
 *
 * @author hal.hildebrand
 */
public interface KERL extends KEL {

    default KeyStateWithEndorsementsAndValidations getKeyStateWithEndorsementsAndValidations(
    EventCoordinates coordinates) {
        var ks = getKeyStateWithAttachments(coordinates);
        if (ks == null) {
            return null;
        }
        return KeyStateWithEndorsementsAndValidations.create(ks.state(), ks.attachments().endorsements(),
                                                             getValidations(coordinates));
    }

    Map<EventCoordinates, JohnHancock> getValidations(EventCoordinates coordinates);

    default List<EventWithAttachments> kerl(Identifier identifier) {
        // TODO use a real DB query instead of this really expensive iterative lookup
        var ks = getKeyState(identifier);
        if (ks == null) {
            return Collections.emptyList();
        }
        var ke = getKeyEvent(ks.getCoordinates());
        return kerl(ke);
    }

    private EventWithAttachments completeKerl(EventCoordinates c, List<EventWithAttachments> result) {
        if (c == null) {
            return null;
        }
        var a = getAttachment(c);
        var e = getKeyEvent(c);
        if (e == null) {
            return null;
        }
        result.add(new EventWithAttachments(e, a));
        return completeKerl(e.getPrevious(), result);
    }

    private List<EventWithAttachments> kerl(KeyEvent event) {
        var result = new ArrayList<EventWithAttachments>();
        Attachment a = getAttachment(event.getCoordinates());

        result.add(new EventWithAttachments(event, a));
        var c = event.getPrevious();
        completeKerl(c, result);
        Collections.reverse(result);
        return result;
    }

    interface AppendKERL extends KERL, AppendKEL {
        Void append(List<AttachmentEvent> events);

        Void appendValidations(EventCoordinates coordinates, Map<EventCoordinates, JohnHancock> validations);

    }

    record EventWithAttachments(KeyEvent event, Attachment attachments) {

        public KeyEventWithAttachments toKeyEvente() {
            var builder = KeyEventWithAttachments.newBuilder();
            event.setEventOf(builder);
            if (attachments != null) {
                builder.setAttachment(attachments.toAttachemente());
            }
            return builder.build();
        }
    }
}
