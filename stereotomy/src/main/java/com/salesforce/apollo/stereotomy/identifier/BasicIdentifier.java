/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import java.security.PublicKey;
import java.util.Objects;

/**
 * @author hal.hildebrand
 *
 */
public class BasicIdentifier implements Identifier {

    private final PublicKey publicKey;

    public BasicIdentifier(PublicKey publicKey) {
        this.publicKey = publicKey;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BasicIdentifier)) {
            return false;
        }
        BasicIdentifier other = (BasicIdentifier) obj;
        return Objects.equals(publicKey, other.publicKey);
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(publicKey);
    }

    @Override
    public boolean isTransferable() {
        return false;
    }

}
