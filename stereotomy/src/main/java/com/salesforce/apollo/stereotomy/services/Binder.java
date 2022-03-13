/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.InceptionEventImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.utils.Pair;

/**
 * Bindings may be made between non transferable identifiers and any of the
 * available Bound value types. Bindings are the signed Bound value by the key
 * of the identifier of the binding.
 * 
 * @author hal.hildebrand
 * 
 */
public interface Binder {
    interface BinderService {
        void bind(Binding binding) throws TimeoutException;

        void unbind(Identifier identifier) throws TimeoutException;
    }

    public record Bound(InceptionEvent identifier, Map<String, URI> uris) {}

    public record Binding(Bound value, JohnHancock signature) {
        public static Binding from(com.salesfoce.apollo.stereotomy.event.proto.Binding binding) {
            Map<String, URI> uris = binding.getValue()
                                           .getUriMap()
                                           .entrySet()
                                           .stream()
                                           .map(e -> new Pair<String, URI>(e.getKey(), toUri(e.getValue())))
                                           .filter(p -> p.b != null)
                                           .collect(Collectors.toMap(p -> p.a, p -> p.b));

            return binding.equals(com.salesfoce.apollo.stereotomy.event.proto.Binding.getDefaultInstance()) ? null
                                                                                                            : new Binding(new Bound(new InceptionEventImpl(binding.getValue()
                                                                                                                                                                  .getIdentifier()),
                                                                                                                                    uris),
                                                                                                                          JohnHancock.from(binding.getSignature()));
        }

        private static URI toUri(String uri) {
            try {
                return new URI(uri);
            } catch (URISyntaxException e) {
                return null;
            }
        }
    }
}
