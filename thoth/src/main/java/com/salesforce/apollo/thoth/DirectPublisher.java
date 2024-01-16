/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import com.salesforce.apollo.cryptography.Digest;
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
    private final Digest           member;

    public DirectPublisher(Digest member, ProtoKERLAdapter kerl) {
        this.member = member;
        this.kerl = kerl;
    }

    @Override
    public void publish(KERL_ kerl_, List<Validations> validations) {
        log.info("publishing KERL[{}] and validations[{}] on: {}", kerl_.getEventsCount(), validations.size(), member);
        validations.forEach(kerl::appendValidations);
        log.info("published KERL[{}] and validations[{}] on: {}", kerl_.getEventsCount(), validations.size(), member);
        kerl.append(kerl_);
    }

    @Override
    public void publishAttachments(List<AttachmentEvent> attachments) {
        log.info("Publishing attachments[{}] on: {}", attachments.size(), member);
        kerl.appendAttachments(attachments);
        log.info("Published attachments[{}] on: {}", attachments.size(), member);
    }

    @Override
    public void publishEvents(List<KeyEvent_> events, List<Validations> validations) {
        log.info("Publishing events[{}], validations[{}] on: {}", events.size(), validations.size(), member);
        validations.forEach(kerl::appendValidations);
        kerl.append(events);
        log.info("Published events[{}], validations[{}] on: {}", events.size(), validations.size(), member);
    }
}
