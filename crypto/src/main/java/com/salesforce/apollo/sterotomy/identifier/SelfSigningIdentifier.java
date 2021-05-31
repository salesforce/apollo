/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.sterotomy.identifier;

import java.util.Objects;

import com.salesforce.apollo.sterotomy.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public class SelfSigningIdentifier implements Identifier {

    private final JohnHancock signature;

    public SelfSigningIdentifier(JohnHancock signature) {
        this.signature = signature;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SelfSigningIdentifier)) {
            return false;
        }
        SelfSigningIdentifier other = (SelfSigningIdentifier) obj;
        return Objects.equals(signature, other.signature);
    }

    public JohnHancock getSignature() {
        return signature;
    }

    @Override
    public int hashCode() {
        return Objects.hash(signature);
    }

    @Override
    public boolean isTransferable() {
        return true;
    }

}
