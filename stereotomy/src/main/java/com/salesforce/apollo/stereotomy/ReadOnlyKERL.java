/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author hal.hildebrand
 */
public class ReadOnlyKERL extends DelegatedKERL {

    public ReadOnlyKERL(KERL delegate) {
        super(delegate);
    }

    private static <T> T complete() {
        return null;
    }

    private static <T> List<T> completeList() {
        return Collections.emptyList();
    }

    @Override
    public KeyState append(KeyEvent event) {
        return complete();
    }

    @Override
    public List<KeyState> append(KeyEvent... events) {
        return completeList();
    }

    @Override
    public Void append(List<AttachmentEvent> events) {
        return complete();
    }

    @Override
    public List<KeyState> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        return completeList();
    }

    @Override
    public Void appendValidations(EventCoordinates coordinates,
                                  Map<EventCoordinates, JohnHancock> validations) {
        return complete();
    }
}
