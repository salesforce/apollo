/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;

/**
 * @author hal.hildebrand
 *
 */
public class AniTest extends AbstractDhtTest {

    @Test
    public void smokin() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        List<? extends Identifier> validators = new ArrayList<>();
        SigningThreshold threshold = SigningThreshold.unweighted(0);
        Map<Digest, Ani> anis = dhts.entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(e -> e.getKey(),
                                                              e -> new Ani(validators, threshold, e.getValue())));
        routers.values().forEach(lr -> lr.start());
        dhts.values().forEach(e -> e.start(Executors.newSingleThreadScheduledExecutor(), Duration.ofSeconds(1)));

        var dht = dhts.values().stream().findFirst().get();
        var ani = anis.values().stream().findFirst().get();

        // inception
        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);

        dht.append(Collections.singletonList(inception.toKeyEvent_())).get();
        assertTrue(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertTrue(ani.getValidation(Duration.ofSeconds(10)).validate(inception));
    }
}
