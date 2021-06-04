/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.List;

import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.specification.InteractionSpecification;
import com.salesforce.apollo.stereotomy.specification.RotationSpecification.Builder;

/**
 * @author hal.hildebrand
 *
 */
public interface ControllableIdentifier extends KeyState {

    void rotate(Builder spec, SignatureAlgorithm signatureAlgorithm);

    void rotate(List<Seal> seals, Builder spec, SignatureAlgorithm signatureAlgorithm);

    void seal(List<Seal> seals, InteractionSpecification.Builder spec);

    EventSignature sign(KeyEvent event);

}
