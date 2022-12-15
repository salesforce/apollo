/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static java.util.concurrent.CompletableFuture.allOf;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;

/**
 * @author hal.hildebrand
 *
 */
public class DirectPublisher implements ProtoEventObserver {
    private final ProtoKERLAdapter kerl;

    public DirectPublisher(ProtoKERLAdapter kerl) {
        super();
        this.kerl = kerl;
    }

    @Override
    public CompletableFuture<Void> publish(KERL_ kerl_, List<Validations> validations) {
        var valids = validations.stream().map(v -> kerl.appendValidations(v)).toList();
        return allOf(valids.toArray(new CompletableFuture[valids.size()])).thenCompose(v -> kerl.append(kerl_)
                                                                                                .thenApply(ks -> null));
    }

    @Override
    public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments) {
        return kerl.appendAttachments(attachments).thenApply(e -> null);
    }

    @Override
    public CompletableFuture<Void> publishEvents(List<KeyEvent_> events, List<Validations> validations) {
        var valids = validations.stream().map(v -> kerl.appendValidations(v)).toList();
        return allOf(valids.toArray(new CompletableFuture[valids.size()])).thenCompose(v -> kerl.append(events)
                                                                                                .thenApply(ks -> null));
    }
}
