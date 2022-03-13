/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.nio.file.Path;

import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * This is the top level container model for the distributed system. It
 * represents the top level domain that contains every sub domain. The
 * membership of this domain is the entirety of all members in the system. The
 * Context of this domain is also the foundational fireflies membership domain
 * of the entire system.
 * 
 * @author hal.hildebrand
 *
 */
public class RootDomain extends Domain {

    public RootDomain(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                      Builder params, com.salesforce.apollo.choam.Parameters.RuntimeParameters.Builder runtime) {
        this(overlay, id, params, "jdbc:h2:mem:", tempDirOf(id), runtime);
    }

    public RootDomain(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                      Builder params, Path checkpointBaseDir,
                      com.salesforce.apollo.choam.Parameters.RuntimeParameters.Builder runtime) {
        this(overlay, id, params, "jdbc:h2:mem:", checkpointBaseDir, runtime);
    }

    public RootDomain(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                      Builder params, String dbURL, Path checkpointBaseDir,
                      com.salesforce.apollo.choam.Parameters.RuntimeParameters.Builder runtime) {
        super(overlay, id, params, dbURL, checkpointBaseDir, runtime);
    }
}
