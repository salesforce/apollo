/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.Collections;
import java.util.function.Consumer;

import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class GenesisContext extends ViewContext {

    public GenesisContext(Context<Member> context, Parameters params, Signer signer,
                          Consumer<HashedCertifiedBlock> publisher) {
        super(context, params, signer, Collections.emptyMap(), publisher);
    }

    @Override
    public boolean validate(Member m, PubKey encoded, JohnHancock sig) {
        return true; // no validators on genesis
    }

}
