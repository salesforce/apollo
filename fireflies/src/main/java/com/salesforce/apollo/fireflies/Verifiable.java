/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.security.SignatureException;

import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avro.Signed;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Verifiable {
    byte[] content();

    byte[] getSignature();

    default Signed getSigned() {
        return new Signed(ByteBuffer.wrap(content()), ByteBuffer.wrap(getSignature()));
    }

    default boolean verify(Signature s) {
        try {
            s.update(content());
            return s.verify(getSignature());
        } catch (SignatureException e) {
            LoggerFactory.getLogger(getClass()).debug("invalid signature", e);
            return false;
        }
    }
}
