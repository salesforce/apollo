/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.jdbc.JdbcConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class ThothTest {
    private static Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    private static final int                                                   CARDINALITY = 100;

    @BeforeAll
    public static void beforeClass() {
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT),
                                            new SecureRandom());
        identities = IntStream.range(0, CARDINALITY)
                              .parallel()
                              .mapToObj(i -> stereotomy.newIdentifier().get())
                              .map(ci -> {
                                  @SuppressWarnings("unchecked")
                                  var casted = (ControlledIdentifier<SelfAddressingIdentifier>) ci;
                                  return casted;
                              })
                              .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                        controlled -> controlled));
    }

    @Test
    public void smokin() throws Exception {
        final var url = "jdbc:h2:mem:thoth_test-smoke;DB_CLOSE_DELAY=-1";
        Context<Member> context = Context.<Member>newBuilder().setCardinality(5).build();
        SigningMember member = new ControlledIdentifierMember(identities.values().stream().findFirst().get());
        JdbcConnection connection = new JdbcConnection(url, new Properties(), "", "", false);
        String prefix = UUID.randomUUID().toString();
        Executor executor = Executors.newCachedThreadPool();
        Router router = new LocalRouter(prefix, ServerConnectionCache.newBuilder().setTarget(2), executor, null);

        final var thoth = new Thoth(context, member, connection, DigestAlgorithm.DEFAULT, router, executor,
                                    Duration.ofMillis(300), null);
    }
}
