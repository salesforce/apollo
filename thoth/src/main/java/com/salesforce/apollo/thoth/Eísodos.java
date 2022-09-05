/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.CompactContext;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserver;

/**
 * @author hal.hildebrand
 *
 */
public class Eísodos {
    private class Service implements EventObserver {

        @Override
        public CompletableFuture<Void> publish(KERL_ kerl, List<Validations> validations, Digest from) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments, Digest from) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<Void> publishEvents(List<KeyEvent_> events, List<Validations> validations,
                                                     Digest from) {
            // TODO Auto-generated method stub
            return null;
        }
    }

    @SuppressWarnings("unused")
    private final KERL                            kerl;
    @SuppressWarnings("unused")
    private final Service                         service    = new Service();
    @SuppressWarnings("unused")
    private final AtomicReference<CompactContext> validators = new AtomicReference<>();

    public Eísodos(KERL kerl) {
        this.kerl = kerl;
    }
}
