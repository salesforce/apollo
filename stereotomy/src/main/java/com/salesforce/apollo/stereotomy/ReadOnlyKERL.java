/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public class ReadOnlyKERL extends DelegatedKERL {

    private static <T> CompletableFuture<T> complete() {
        var fs = new CompletableFuture<T>();
        fs.complete(null);
        return fs;
    }

    private static <T> CompletableFuture<List<T>> completeList() {
        var fs = new CompletableFuture<List<T>>();
        fs.complete(Collections.emptyList());
        return fs;
    }

    public ReadOnlyKERL(KERL delegate) {
        super(delegate);
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        return complete();
    }

    @Override
    public CompletableFuture<List<KeyState>> append(KeyEvent... events) {
        return completeList();
    }

    @Override
    public CompletableFuture<Void> append(List<AttachmentEvent> events) {
        return complete();
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        return completeList();
    }

    @Override
    public CompletableFuture<Void> appendValidations(EventCoordinates coordinates,
                                                     Map<EventCoordinates, JohnHancock> validations) {
        return complete();
    }

}
