/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.util.Optional;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import com.google.protobuf.Any;
import com.salesfoce.apollo.stereotomy.event.proto.Resolve;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.service.Resolver;

/**
 * @author hal.hildebrand
 *
 */
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

    private final MVMap<byte[], byte[]> bindings;
    private final Context<Member>       context;
    private final KEL           kel;
    private final String                MAP_TEMPLATE = "%s-thoth.bindgs";
    private final SigningMember         node;

    public Thoth(Context<Member> context, SigningMember node, KEL kel, MVStore store) {
        this.context = context;
        this.node = node;
        this.kel = kel;
        bindings = store.openMap(String.format(MAP_TEMPLATE, qb64(context.getId())));
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
        return kel.getKeyState(prefix);
    }
}
