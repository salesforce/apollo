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
 * The logical domain of the current "Process" - OS and Simulation defined,
 * 'natch
 * 
 * @author hal.hildebrand
 *
 */
public class ProcessDomain extends Domain {

    public ProcessDomain(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                         Builder params, com.salesforce.apollo.choam.Parameters.RuntimeParameters.Builder runtime) {
        this(overlay, id, params, "jdbc:h2:mem:", tempDirOf(id), runtime);
    }

    public ProcessDomain(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                         Builder params, Path checkpointBaseDir,
                         com.salesforce.apollo.choam.Parameters.RuntimeParameters.Builder runtime) {
        this(overlay, id, params, "jdbc:h2:mem:", checkpointBaseDir, runtime);
    }

    public ProcessDomain(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                         Builder params, String dbURL, Path checkpointBaseDir,
                         com.salesforce.apollo.choam.Parameters.RuntimeParameters.Builder runtime) {
        super(overlay, id, params, dbURL, checkpointBaseDir, runtime);
    }
}
