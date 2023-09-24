/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.Router.ServiceRouting;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserver;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverClient;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverServer;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverService;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;

import java.util.List;

/**
 * @author hal.hildebrand
 */
public class Publisher implements ProtoEventObserver {

    private final CommonCommunications<EventObserverService, EventObserver> comms;
    private final Digest context;
    private final ProtoKERLAdapter kerl;
    private final EventObserver service;
    public Publisher(SigningMember member, ProtoKERLAdapter kerl, Router router, Digest context) {
        this.kerl = kerl;
        this.context = context;
        service = new Service();
        comms = router.create(member, context, service, service.getClass().getSimpleName(),
                r -> new EventObserverServer(r, router.getClientIdentityProvider(), null), null,
                EventObserverClient.getLocalLoopback(this, member));
    }

    @Override
    public void publish(KERL_ kerl_, List<Validations> validations) {
        var valids = validations.stream().map(v -> kerl.appendValidations(v)).toList();
        kerl.append(kerl_);
    }

    @Override
    public void publishAttachments(List<AttachmentEvent> attachments) {
        kerl.appendAttachments(attachments);
    }

    @Override
    public void publishEvents(List<KeyEvent_> events, List<Validations> validations) {
        validations.forEach(v -> kerl.appendValidations(v));
    }

    public void start() {
        comms.register(context, service);
    }

    public void stop() {
        comms.deregister(context);
    }

    private class Service implements EventObserver, ServiceRouting {
        @Override
        public void publish(KERL_ kerl, List<Validations> validations, Digest from) {
            Publisher.this.publish(kerl, validations);
        }

        @Override
        public void publishAttachments(List<AttachmentEvent> attachments, Digest from) {
            Publisher.this.publishAttachments(attachments);
        }

        @Override
        public void publishEvents(List<KeyEvent_> events, List<Validations> validations,
                                  Digest from) {
            Publisher.this.publishEvents(events, validations);
        }
    }
}
