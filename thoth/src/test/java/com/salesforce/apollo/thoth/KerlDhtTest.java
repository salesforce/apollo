/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class KerlDhtTest extends AbstractDhtTest {

    @Test
    public void smokin() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        routers.values().forEach(r -> r.start());
        dhts.values()
            .forEach(dht -> dht.start(Executors.newScheduledThreadPool(2, Utils.virtualThreadFactory()),
                                      Duration.ofSeconds(1)));

        // inception
        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);

        var dht = dhts.values().stream().findFirst().get();

        dht.append(Collections.singletonList(inception.toKeyEvent_())).get();
        var lookup = dht.getKeyEvent(inception.getCoordinates().toEventCoords()).get();
        assertNotNull(lookup);
        assertEquals(inception.toKeyEvent_(), lookup);
    }
}
