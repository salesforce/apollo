/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.AttachmentImpl;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

/**
 * 
 * The control interface for a node
 *
 * @author hal.hildebrand
 *
 */
public class Thoth {

    private static final Logger log = LoggerFactory.getLogger(Thoth.class);

    private volatile SelfAddressingIdentifier                       controller;
    private volatile ControlledIdentifier<SelfAddressingIdentifier> identifier;
    private volatile Consumer<EventCoordinates>                     pending;
    private final Stereotomy                                        stereotomy;

    public Thoth(Stereotomy stereotomy) {
        this.stereotomy = stereotomy;
    }

    public void commit(EventCoordinates coords) {
        final var commiting = pending;

        if (commiting == null) {
            log.info("No pending commitment for delegation: {}", coords);
        }

        commiting.accept(coords);
    }

    public SelfAddressingIdentifier identifier() {
        if (identifier == null) {
            throw new IllegalStateException("Identifier has not been established");
        }
        return identifier.getIdentifier();
    }

    public DelegatedInceptionEvent inception(SelfAddressingIdentifier controller,
                                             IdentifierSpecification.Builder<SelfAddressingIdentifier> specification) {
        final var inception = stereotomy.newDelegatedIdentifier(controller, specification);
        pending = inception(inception);
        return inception;
    }

    public ControlledIdentifierMember member() {
        final var id = identifier;
        if (id == null) {
            throw new IllegalStateException("Inception has not been commited");
        }
        return new ControlledIdentifierMember(id);
    }

    public CompletableFuture<DelegatedRotationEvent> rotate(RotationSpecification.Builder specification) {
        if (identifier == null) {
            throw new IllegalStateException("Identifier has not been established");
        }
        if (pending != null) {
            throw new IllegalStateException("Still pending previous commitment");
        }
        final var rot = identifier.delegateRotate(specification);
        rot.whenComplete((rotation, t) -> {
            pending = rotation(rotation);
        });
        return rot;
    }

    private Consumer<EventCoordinates> inception(DelegatedInceptionEvent incp) {
        return coordinates -> {
            var commitment = ProtobufEventFactory.INSTANCE.attachment(incp,
                                                                      new AttachmentImpl(Seal.EventSeal.construct(coordinates.getIdentifier(),
                                                                                                                  coordinates.getDigest(),
                                                                                                                  coordinates.getSequenceNumber()
                                                                                                                             .longValue())));
            try {
                stereotomy.commit(incp, commitment).whenComplete((cid, t) -> {
                    if (t != null) {
                        log.error("Unable to commit inception: {}", incp, t);
                        return;
                    }
                    identifier = cid;
                    controller = (SelfAddressingIdentifier) identifier.getDelegatingIdentifier().get();
                    pending = null;
                    log.info("Created delegated identifier: {} controller: {}", identifier.getIdentifier(), controller);
                }).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Unable to commit inception", e.getCause());
            }
        };
    }

    private Consumer<EventCoordinates> rotation(DelegatedRotationEvent rot) {
        return coordinates -> {
            var commitment = ProtobufEventFactory.INSTANCE.attachment(rot,
                                                                      new AttachmentImpl(Seal.EventSeal.construct(coordinates.getIdentifier(),
                                                                                                                  coordinates.getDigest(),
                                                                                                                  coordinates.getSequenceNumber()
                                                                                                                             .longValue())));
            identifier.commit(rot, commitment).whenComplete((cid, t) -> {
                if (t != null) {
                    log.error("Unable to commit rotation: {} for: {} controller: {}", rot, identifier.getIdentifier(),
                              controller, t);
                    return;
                }
                pending = null;
                log.info("Rotated delegated identifier: {} controller: {}", identifier.getCoordinates(), controller,
                         identifier.getCoordinates());
            });
        };
    }
}
