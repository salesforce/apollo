/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import com.google.protobuf.Empty;
import com.salesforce.apollo.stereotomy.event.proto.*;

import java.util.List;

/**
 * @author hal.hildebrand
 */
public interface ProtoKERLService extends ProtoKERLProvider {

    List<KeyState_> append(KERL_ kerl);

    List<KeyState_> append(List<KeyEvent_> events);

    List<KeyState_> append(List<KeyEvent_> events, List<AttachmentEvent> attachments);

    Empty appendAttachments(List<AttachmentEvent> attachments);

    Empty appendValidations(Validations validations);
}
