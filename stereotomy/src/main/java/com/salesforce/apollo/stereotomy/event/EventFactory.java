/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

public interface EventFactory {

    AttachmentEvent attachment(EstablishmentEvent event, Attachment attachment);

    InceptionEvent inception(Identifier identifier, IdentifierSpecification specification);

    KeyEvent interaction(InteractionSpecification specification);

    RotationEvent rotation(RotationSpecification specification, boolean delegated);

}
