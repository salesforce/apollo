/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.nio.file.Path;

import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class SubDomain extends Domain {
    @SuppressWarnings("unused")
    private final Domain parentDomain;

    public SubDomain(ControlledIdentifier<SelfAddressingIdentifier> id, Builder params,
                     RuntimeParameters.Builder runtime) {
        this(id, params, "jdbc:h2:mem:", tempDirOf(id), runtime);
    }

    public SubDomain(ControlledIdentifier<SelfAddressingIdentifier> id, Builder params, Path checkpointBaseDir,
                     Parameters.RuntimeParameters.Builder runtime) {
        this(id, params, "jdbc:h2:mem:", checkpointBaseDir, runtime);
    }

    public SubDomain(ControlledIdentifier<SelfAddressingIdentifier> id, Builder params, String dbURL,
                     Path checkpointBaseDir, Parameters.RuntimeParameters.Builder runtime) {
        super(id, params, dbURL, checkpointBaseDir, runtime);
        this.parentDomain = null;
    }
}
