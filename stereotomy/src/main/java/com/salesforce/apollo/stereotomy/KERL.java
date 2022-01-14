/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * The Key Event Receipt Log
 * 
 * @author hal.hildebrand
 *
 */
public interface KERL extends KEL {

    record EventWithAttachments(KeyEvent event, Attachment attachments) {

        public com.salesfoce.apollo.stereotomy.event.proto.KeyEvent toKeyEvente() {
            var builder = com.salesfoce.apollo.stereotomy.event.proto.KeyEvent.newBuilder();
            event.setEventOf(builder);
            return builder.setAttachment(attachments.toAttachemente()).build();
        }}

    CompletableFuture<Void> append(AttachmentEvent event);

    Optional<List<EventWithAttachments>> kerl(Identifier identifier);

}
