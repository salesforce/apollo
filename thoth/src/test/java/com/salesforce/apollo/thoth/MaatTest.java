/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.security.SecureRandom;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class MaatTest {

    @Test
    public void smokin() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var kerl_ = new MemKERL(DigestAlgorithm.DEFAULT);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), kerl_, entropy);
        var context = Context.newBuilder().setCardinality(4).build();
        for (int i = 0; i < 4; i++) {
            context.activate(new ControlledIdentifierMember(stereotomy.newIdentifier().get()));
        }
        var maat = new Maat(context, kerl_, kerl_);

        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var inception = AbstractDhtTest.inception(specification, initialKeyPair, ProtobufEventFactory.INSTANCE,
                                                  nextKeyPair);
        var digest = ((SelfAddressingIdentifier) inception.getIdentifier()).getDigest();

        var serialized = inception.getBytes();
        var validations = new HashMap<EventCoordinates, JohnHancock>();

        context.successors(digest).stream().map(m -> (ControlledIdentifierMember) m).forEach(m -> {
            validations.put(m.getEvent().getCoordinates(), m.sign(serialized));
        });

        var inceptionState = maat.append(inception).get();
        assertNull(inceptionState, "Should not have succeeded appending of test event");

        kerl_.appendValidations(inception.getCoordinates(), validations).get();

        inceptionState = maat.append(inception).get();
        assertNotNull(inceptionState, "Should have succeeded appending of test event");
    }
}
