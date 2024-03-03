/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author hal.hildebrand
 */
public class CompactRingTest {

    @Test
    public void smokin() throws Exception {
        var cardinality = 1_000;
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        var members = IntStream.range(0, cardinality).mapToObj(i -> {
            return new ControlledIdentifierMember(stereotomy.newIdentifier());
        }).toList();
        final var ctxBuilder = DynamicContext.newBuilder().setCardinality(cardinality);
        var context = ctxBuilder.build();
        members.forEach(m -> context.activate(m));
        final var compact = CompactContext.newBuilder(ctxBuilder)
                                          .setMembers(members.stream().map(m -> m.getId()).toList())
                                          .build();
        assertEquals(context.getRingCount(), compact.getRingCount());

        for (int i = 0; i < context.getRingCount(); i++) {
            assertEquals(context.stream(i).map(m -> m.getId()).toList(), compact.ring(i).stream().toList(),
                         "Ring " + i + " mismatched");
        }
    }
}
