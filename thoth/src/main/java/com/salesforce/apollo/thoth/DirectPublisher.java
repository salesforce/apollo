/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;

import java.util.List;

/**
 * @author hal.hildebrand
 */
public class DirectPublisher implements ProtoEventObserver {
    private final ProtoKERLAdapter kerl;

    public DirectPublisher(ProtoKERLAdapter kerl) {
        super();
        this.kerl = kerl;
    }

    @Override
    public void publish(KERL_ kerl_, List<Validations> validations) {
        validations.stream().forEach(v -> kerl.appendValidations(v));
        kerl.append(kerl_);
    }

    @Override
    public void publishAttachments(List<AttachmentEvent> attachments) {
        kerl.appendAttachments(attachments);
    }

    @Override
    public void publishEvents(List<KeyEvent_> events, List<Validations> validations) {
        validations.forEach(v -> kerl.appendValidations(v));
        kerl.append(events);
    }
}
