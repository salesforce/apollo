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
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.Router.CommonCommunications;
import com.salesforce.apollo.archipelago.Router.ServiceRouting;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserver;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverClient;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverServer;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverService;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;

/**
 * @author hal.hildebrand
 *
 */
public class Publisher implements ProtoEventObserver {

    private class Service implements EventObserver, ServiceRouting {
        @Override
        public CompletableFuture<Void> publish(KERL_ kerl, List<Validations> validations, Digest from) {
            return Publisher.this.publish(kerl, validations);
        }

        @Override
        public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments, Digest from) {
            return Publisher.this.publishAttachments(attachments);
        }

        @Override
        public CompletableFuture<Void> publishEvents(List<KeyEvent_> events, List<Validations> validations,
                                                     Digest from) {
            return Publisher.this.publishEvents(events, validations);
        }
    }

    private final CommonCommunications<EventObserverService, EventObserver> comms;
    private final Digest                                                    context;
    private final ProtoKERLAdapter                                          kerl;
    private final EventObserver                                             service;

    public Publisher(SigningMember member, ProtoKERLAdapter kerl, Router router, Digest context) {
        this.kerl = kerl;
        this.context = context;
        service = new Service();
        comms = router.create(member, context, service, service.getClass().getSimpleName(),
                              r -> new EventObserverServer(r, router.getClientIdentityProvider(), null), null,
                              EventObserverClient.getLocalLoopback(this, member));
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

    public void start() {
        comms.register(context, service);
    }

    public void stop() {
        comms.deregister(context);
    }
}
