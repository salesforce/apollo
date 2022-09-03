/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.thoth.Gorgoneion.Parameters;

/**
 * @author hal.hildebrand
 *
 */
public class GorgoneionTest {

    @Test
    public void smokin() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        LocalRouter router = new LocalRouter(UUID.randomUUID().toString(), new ConcurrentSkipListMap<>(),
                                             ServerConnectionCache.newBuilder().setTarget(2), ForkJoinPool.commonPool(),
                                             null);
        ControlledIdentifierMember member = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var validating = member.getIdentifier()
                               .newIdentifier(IdentifierSpecification.<SelfAddressingIdentifier>newBuilder())
                               .get();
        var context = Context.<Member>newBuilder().setCardinality(1).build();
        context.activate(member);
        var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        Parameters params = Parameters.newBuilder().setVerifier(sa -> {
            var fs = new CompletableFuture<Boolean>();
            fs.complete(true);
            return fs;
        }).setEntropy(entropy).build();
        var gorgon = new Gorgoneion(member, validating, context, router, kerl, router, params, null);
        assertNotNull(gorgon);
    }
}
