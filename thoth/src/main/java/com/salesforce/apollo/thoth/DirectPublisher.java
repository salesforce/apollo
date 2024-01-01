/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import com.salesforce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.proto.KERL_;
import com.salesforce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesforce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author hal.hildebrand
 */
public class DirectPublisher implements ProtoEventObserver {
    private final static Logger log = LoggerFactory.getLogger(DirectPublisher.class);

    private final ProtoKERLAdapter kerl;

    public DirectPublisher(ProtoKERLAdapter kerl) {
        this.kerl = kerl;
    }

    @Override
    public void publish(KERL_ kerl_, List<Validations> validations) {
        log.info("publishing KERL[{}] and validations[{}]", kerl_.getEventsCount(), validations.size());
        validations.stream().forEach(v -> kerl.appendValidations(v));
        log.info("published KERL[{}] and validations[{}]", kerl_.getEventsCount(), validations.size());
        kerl.append(kerl_);
    }

    @Override
    public void publishAttachments(List<AttachmentEvent> attachments) {
        log.info("Publishing attachments[{}]", attachments.size());
        kerl.appendAttachments(attachments);
        log.info("Published attachments[{}]", attachments.size());
    }

    @Override
    public void publishEvents(List<KeyEvent_> events, List<Validations> validations) {
        log.info("Publishing events[{}], validations[{}]", events.size(), validations.size());
        validations.forEach(v -> kerl.appendValidations(v));
        kerl.append(events);
        log.info("Published events[{}], validations[{}]", events.size(), validations.size());
    }
}
