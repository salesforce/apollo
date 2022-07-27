/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.stereotomy.caching;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class CachingKERL extends CachingKEL<KERL> implements KERL {

    public CachingKERL(Function<Function<KERL, ?>, ?> kelSupplier) {
        super(kelSupplier);
    }

    public CachingKERL(Function<Function<KERL, ?>, ?> kelSupplier, Caffeine<EventCoordinates, KeyState> builder,
                       Caffeine<Identifier, KeyState> curBuilder, Caffeine<EventCoordinates, KeyEvent> eventBuilder) {
        super(kelSupplier, builder, curBuilder, eventBuilder);
    }

    @Override
    public CompletableFuture<Void> append(List<AttachmentEvent> event) {
        try {
            return complete(kerl -> kerl.append(event));
        } catch (Throwable t) {
            var fs = new CompletableFuture<Void>();
            fs.completeExceptionally(t);
            return fs;
        }
    }

    @Override
    public CompletableFuture<List<EventWithAttachments>> kerl(Identifier identifier) {
        return complete(kerl -> kerl.kerl(identifier));
    }

}
