/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.security.KeyPair;
import java.util.List;
import java.util.Optional;

import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.thoth.proto.Signatures;
import com.salesfoce.apollo.thoth.proto.Validated;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.event.protobuf.InteractionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * The witness and validation service in Thoth
 * 
 * @author hal.hildebrand
 *
 */
public class Sakshi {
    private final ControlledIdentifier<SelfAddressingIdentifier> validator;
    private final KeyPair                                        witness;

    public Sakshi(ControlledIdentifier<SelfAddressingIdentifier> validator, KeyPair keyPair) {
        this.validator = validator;
        this.witness = keyPair;
    }

    public Ident getWitness() {
        return new BasicIdentifier(witness.getPublic()).toIdent();
    }

    public Validated validate(List<KeyEventWithAttachments> events) {
        final var builder = Validated.newBuilder();
        events.forEach(event -> {
            var valid = validate(event);
            if (valid.isEmpty()) {
                builder.addSignatures(Sig.getDefaultInstance());
            } else {
                builder.addSignatures(valid.get().toSig()).build();
            }
        });
        return builder.build();
    }

    public Optional<Sig> witness(KeyEvent_ evente, Ident identifier) {
        if (!getWitness().equals(identifier)) {
            return Optional.empty();
        }
        var signer = new SignerImpl(witness.getPrivate());
        return Optional.of(signer.sign(evente.toByteString()).toSig());
    }

    private Optional<JohnHancock> validate(KeyEventWithAttachments kea) {
        if (!witnessed(kea)) {
            return Optional.empty();
        }
        Optional<Signer> signer = validator.getSigner();
        if (signer.isEmpty()) {
            return Optional.empty();
        }
        KeyEvent_ evente = switch (kea.getEventCase()) {
        case INCEPTION -> ProtobufEventFactory.toKeyEvent(kea.getInception()).toKeyEvent_();
        case INTERACTION -> new InteractionEventImpl(kea.getInteraction()).toKeyEvent_();
        case ROTATION -> ProtobufEventFactory.toKeyEvent(kea.getRotation()).toKeyEvent_();
        default -> null;
        };
        return (evente == null) ? Optional.empty() : Optional.of(signer.get().sign(evente.toByteString()));
    }

    // Confirm that the attachments witness the event
    private boolean witnessed(KeyEventWithAttachments kea) {
        // TODO
        return true;
    }

    public Signatures witness(List<KeyEvent_> keyEventList, Ident identifier) {
        // TODO Auto-generated method stub
        return null;
    }
}
