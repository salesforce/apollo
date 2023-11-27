/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.Collections;

import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.cryptography.Signer;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class GenesisContext extends ViewContext {

    public GenesisContext(Context<Member> context, Parameters params, Signer signer, BlockProducer blockProducer) {
        super(context, params, signer, Collections.emptyMap(), blockProducer);
    }

    @Override
    protected Verifier verifierOf(Validate validate) {
        return new Verifier.MockVerifier();
    }
}
