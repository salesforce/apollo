/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The control interface for a node
 *
 * @author hal.hildebrand
 */
public class Thoth {
    private static final Logger                                                   log         = LoggerFactory.getLogger(
    Thoth.class);
    private final        Stereotomy                                               stereotomy;
    private final        Consumer<ControlledIdentifier<SelfAddressingIdentifier>> onInception;
    private volatile     SelfAddressingIdentifier                                 controller;
    private volatile     ControlledIdentifier<SelfAddressingIdentifier>           identifier;
    private volatile     Consumer<EventCoordinates>                               pending;
    private              AtomicBoolean                                            initialized = new AtomicBoolean();

    public Thoth(Stereotomy stereotomy) {
        this(stereotomy, identifier -> {
        });
    }

    public Thoth(Stereotomy stereotomy, Consumer<ControlledIdentifier<SelfAddressingIdentifier>> onInception) {
        this.stereotomy = stereotomy;
        this.onInception = onInception;
    }

    public void commit(EventCoordinates coords) {
        final var commiting = pending;

        if (commiting == null) {
            log.info("No pending commitment for delegation: {}", coords);
        }

        commiting.accept(coords);
    }

    public SelfAddressingIdentifier identifier() {
        final var current = identifier;
        if (current == null) {
            throw new IllegalStateException("Identifier has not been established");
        }
        return current.getIdentifier();
    }

    public DelegatedInceptionEvent inception(SelfAddressingIdentifier controller,
                                             IdentifierSpecification.Builder<SelfAddressingIdentifier> specification) {
        if (initialized.get()) {
            throw new IllegalStateException("Already initialized for: " + identifier);
        }
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

    public DelegatedRotationEvent rotate(RotationSpecification.Builder specification) {
        if (identifier == null) {
            throw new IllegalStateException("Identifier has not been established");
        }
        if (pending != null) {
            throw new IllegalStateException("Still pending previous commitment");
        }
        final var rot = identifier.delegateRotate(specification);
        pending = rotation(rot);
        return rot;
    }

    private Consumer<EventCoordinates> inception(DelegatedInceptionEvent incp) {
        return coordinates -> {
            if (!initialized.compareAndSet(false, true)) {
                return;
            }
            var commitment = ProtobufEventFactory.INSTANCE.attachment(incp, new AttachmentImpl(
            Seal.EventSeal.construct(coordinates.getIdentifier(), coordinates.getDigest(),
                                     coordinates.getSequenceNumber().longValue())));
            ControlledIdentifier<SelfAddressingIdentifier> cid = stereotomy.commit(incp, commitment);
            identifier = cid;
            controller = (SelfAddressingIdentifier) identifier.getDelegatingIdentifier().get();
            pending = null;
            if (onInception != null) {
                log.info("Notifying inception complete for: {} controller: {}", identifier.getIdentifier(), controller);
                onInception.accept(identifier);
            }
            log.info("Created delegated identifier: {} controller: {}", identifier.getIdentifier(), controller);
        };
    }

    private Consumer<EventCoordinates> rotation(DelegatedRotationEvent rot) {
        return coordinates -> {
            var commitment = ProtobufEventFactory.INSTANCE.attachment(rot, new AttachmentImpl(
            Seal.EventSeal.construct(coordinates.getIdentifier(), coordinates.getDigest(),
                                     coordinates.getSequenceNumber().longValue())));
            Void cid = identifier.commit(rot, commitment);
            pending = null;
            log.info("Rotated delegated identifier: {} controller: {}", identifier.getCoordinates(), controller,
                     identifier.getCoordinates());
        };
    }
}
