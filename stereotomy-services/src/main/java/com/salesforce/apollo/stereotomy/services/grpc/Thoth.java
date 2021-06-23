/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.Any;
import com.salesfoce.apollo.stereotomy.event.proto.Bound;
import com.salesfoce.apollo.stereotomy.event.proto.Resolve;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ghost.Ghost;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.service.Resolver;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class Thoth implements Resolver {

    public class ResolverService {
        public Bound lookup(Resolve query, Member from) {
            Duration timeout = Duration.ofSeconds(query.getTimeout().getSeconds(), query.getTimeout().getNanos());
            try {
                return Thoth.this.lookup(Identifier.from(query.getIdentifier()), timeout)
                                 .orElse(Bound.getDefaultInstance());
            } catch (TimeoutException e) {
                return Bound.getDefaultInstance();
            }
        }

        public com.salesfoce.apollo.stereotomy.event.proto.KeyState resolve(Resolve query, Member from) {
            Duration timeout = Duration.ofSeconds(query.getTimeout().getSeconds(), query.getTimeout().getNanos());
            KeyState resolved;
            try {
                resolved = Thoth.this.resolve(Identifier.from(query.getIdentifier()), timeout).orElse(null);
            } catch (TimeoutException e) {
                return com.salesfoce.apollo.stereotomy.event.proto.KeyState.getDefaultInstance();
            }
            return resolved == null ? com.salesfoce.apollo.stereotomy.event.proto.KeyState.getDefaultInstance()
                    : resolved.convertTo(Format.PROTOBUF);
        }
    }

    private final Context<Member> context;
    private final Ghost           ghost;
    private final SigningMember   node;

    public Thoth(Context<Member> context, SigningMember node, Ghost ghost) {
        this.context = context;
        this.node = node;
        this.ghost = ghost;
    }

    @Override
    public void bind(Identifier prefix, Any value, JohnHancock signature, Duration timeout) throws TimeoutException {
        if (prefix.isTransferable()) {
            throw new IllegalArgumentException("Identifier must be non transferrable: " + prefix);
        }
        Bound bound = Bound.newBuilder()
                           .setPrefix(prefix.toByteString())
                           .setValue(value)
                           .setSignature(signature.toByteString())
                           .build();
        ghost.put(Any.pack(value), timeout);
    }

    @Override
    public Optional<Bound> lookup(Identifier prefix, Duration timeout) throws TimeoutException {
        ghost.get(null, timeout);
        return null;
    }

    @Override
    public Optional<KeyState> resolve(Identifier prefix, Duration timeout) throws TimeoutException {
        return null;
    }
}
