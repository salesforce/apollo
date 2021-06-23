/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.Optional;

import com.google.protobuf.Any;
import com.salesfoce.apollo.stereotomy.event.proto.Resolve;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ghost.Ghost;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.KERL;
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
        public Any lookup(Resolve query, Member from) {
            return Thoth.this.lookup(Identifier.from(query.getIdentifier())).orElse(Any.getDefaultInstance());
        }

        public com.salesfoce.apollo.stereotomy.event.proto.KeyState resolve(Resolve query, Member from) {
            KeyState resolved = Thoth.this.resolve(Identifier.from(query.getIdentifier())).orElse(null);
            return resolved == null ? com.salesfoce.apollo.stereotomy.event.proto.KeyState.getDefaultInstance()
                    : resolved.convertTo(Format.PROTOBUF);
        }
    }

    private final Ghost           ghost;
    private final Context<Member> context;
    private final KERL            kerl;
    private final SigningMember   node;

    public Thoth(Context<Member> context, SigningMember node, KERL kerl, Ghost ghost) {
        this.context = context;
        this.node = node;
        this.kerl = kerl;
        this.ghost = ghost;
    }

    @Override
    public void bind(Identifier prefix, Any value, JohnHancock signature) {
        // TODO Auto-generated method stub

    }

    @Override
    public Optional<Any> lookup(Identifier prefix) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<KeyState> resolve(Identifier prefix) {
        return kerl.getKeyState(prefix);
    }
}