/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.mem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.ResolverService;
import com.salesforce.apollo.stereotomy.services.ResolverService.BinderService;

/**
 * @author hal.hildebrand
 *
 */
public class MemResolver implements ResolverService, BinderService {

    private final KERL                     kerl;
    private final Map<Identifier, Binding> bindings = new ConcurrentHashMap<>();

    public MemResolver(KERL kerl) {
        this.kerl = kerl;
    }

    @Override
    public void bind(Binding binding) throws TimeoutException {
        bindings.put(binding.value().identifier().getIdentifier(), binding);
    }

    @Override
    public void unbind(Identifier identifier) throws TimeoutException {
        bindings.remove(identifier);
    }

    @Override
    public Optional<List<EventWithAttachments>> kerl(Identifier prefix) throws TimeoutException {
        return kerl.kerl(prefix);
    }

    @Override
    public Optional<Binding> lookup(Identifier prefix) throws TimeoutException {
        return Optional.ofNullable(bindings.get(prefix));
    }

    @Override
    public Optional<KeyState> resolve(EventCoordinates coordinates) throws TimeoutException {
        return kerl.getKeyState(coordinates);
    }

    @Override
    public Optional<KeyState> resolve(Identifier prefix) throws TimeoutException {
        return kerl.getKeyState(prefix);
    }

}
