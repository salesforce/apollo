/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLClient;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLServer;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLService;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverClient;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverServer;
import com.salesforce.apollo.stereotomy.services.grpc.observer.EventObserverService;
import com.salesforce.apollo.stereotomy.services.grpc.validation.EventValidationClient;
import com.salesforce.apollo.stereotomy.services.grpc.validation.EventValidationServer;
import com.salesforce.apollo.stereotomy.services.grpc.validation.EventValidationService;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventValidation;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

/**
 * @author hal.hildebrand
 *
 */
public class Thoth implements ProtoKERLService, ProtoEventValidation, ProtoEventObserver {
    private class Service implements ProtoKERLService, ProtoEventValidation, ProtoEventObserver {

        @Override
        public CompletableFuture<List<KeyState_>> append(KERL_ kerl) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<KERL_> getKERL(Ident identifier) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<KeyEvent_> getKeyEvent(Digeste digest) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<List<AttachmentEvent>> publish(KERL_ kerl) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<List<AttachmentEvent>> publishEvents(List<KeyEvent_> events) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<Boolean> validate(KeyEvent_ event) {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private final CommonCommunications<KERLService, ProtoKERLService>                kerlComms;
    private final CommonCommunications<EventObserverService, ProtoEventObserver>     observerComms;
    private final Service                                                            service = new Service();
    private final CommonCommunications<EventValidationService, ProtoEventValidation> validationComms;

    public Thoth(SigningMember member, Context<Member> context, Router router, StereotomyMetrics metrics) {
        observerComms = router.create(member, context.getId(), service, r -> new EventObserverServer(metrics, r),
                                      EventObserverClient.getCreate(context.getId(), metrics), null);
        kerlComms = router.create(member, context.getId(), service, r -> new KERLServer(metrics, r),
                                  KERLClient.getCreate(context.getId(), metrics), null);
        validationComms = router.create(member, context.getId(), service, r -> new EventValidationServer(metrics, r),
                                        EventValidationClient.getCreate(context.getId(), metrics), null);
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(KERL_ kerl) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<KERL_> getKERL(Ident identifier) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<KeyEvent_> getKeyEvent(Digeste digest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<List<AttachmentEvent>> publish(KERL_ kerl) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<List<AttachmentEvent>> publishEvents(List<KeyEvent_> events) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<Boolean> validate(KeyEvent_ event) {
        // TODO Auto-generated method stub
        return null;
    }
}
