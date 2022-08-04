/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.SecureRandom;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class ThothTest {
    private SecureRandom secureRandom;

    @BeforeEach
    public void before() throws Exception {
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
    }

    @Test
    public void smokin() throws Exception {
        var ks = new MemKeyStore();
        var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        Stereotomy stereotomy = new StereotomyImpl(ks, kerl, secureRandom);

        var thoth = new Thoth(stereotomy);

        ControlledIdentifier<SelfAddressingIdentifier> controller = stereotomy.newIdentifier().get();

        // delegated inception
        var incp = thoth.inception(controller.getIdentifier(),
                                   IdentifierSpecification.<SelfAddressingIdentifier>newBuilder());
        assertNotNull(incp);

        var seal = Seal.EventSeal.construct(incp.getIdentifier(), incp.hash(stereotomy.digestAlgorithm()),
                                            incp.getSequenceNumber().longValue());

        var builder = InteractionSpecification.newBuilder().addAllSeals(Collections.singletonList(seal));

        // Commit
        controller.seal(builder).thenAccept(coords -> thoth.commit(coords)).get();
        assertNotNull(thoth.identifier());

        // Delegated rotation
        var rot = thoth.rotate(RotationSpecification.newBuilder()).get();

        assertNotNull(rot);

        seal = Seal.EventSeal.construct(rot.getIdentifier(), rot.hash(stereotomy.digestAlgorithm()),
                                        rot.getSequenceNumber().longValue());

        builder = InteractionSpecification.newBuilder().addAllSeals(Collections.singletonList(seal));

        // Commit
        controller.seal(builder).thenAccept(coords -> thoth.commit(coords)).get();
    }
}
