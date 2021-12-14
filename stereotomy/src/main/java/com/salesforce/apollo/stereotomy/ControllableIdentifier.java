/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.List;
import java.util.Optional;

import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

/**
 * A controlled identifier, representing the current state of the identifier at
 * all times.
 * 
 * @author hal.hildebrand
 *
 */
public interface ControllableIdentifier extends BoundIdentifier {
    /**
     * @return the binding of the identifier to the current key state
     */
    BoundIdentifier bind();

    /**
     * @param keyIndex
     * @return the Signer for the key state binding
     */
    Optional<Signer> getSigner(int keyIndex);

    /**
     * Rotate the current key state
     */
    void rotate();

    /**
     * Rotate the current key state using the supplied seals
     */
    void rotate(List<Seal> seals);

    /**
     * Rotate the current key state using the supplied specification
     */
    void rotate(RotationSpecification.Builder spec);

    /**
     * Publish the SealingEvent using the supplied specification
     */
    void seal(InteractionSpecification.Builder spec);

    /**
     * Publish the SealingEvent using the supplied seals
     */
    void seal(List<Seal> seals);

    /**
     * @return the EventSignature for the supplied event
     */
    Optional<EventSignature> sign(KeyEvent event);

}
