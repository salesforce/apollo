/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.stereotomy.event.proto.Bound;
import com.salesfoce.apollo.stereotomy.event.proto.Resolve;
import com.salesfoce.apollo.utils.proto.Boxed;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ghost.Ghost;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.mvlog.KeyStateImpl;
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

    private final static Logger   log = LoggerFactory.getLogger(Thoth.class);
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
        ghost.bind(qb64(prefix), Any.pack(value), timeout);
    }

    @Override
    public void bind(Identifier prefix, KeyState keystate, Duration timeout) throws TimeoutException {
        if (!prefix.isTransferable()) {
            throw new IllegalArgumentException("Identifier must be transferrable: " + prefix);
        }
        String prefixKey = qb64(prefix);
        ghost.bind(prefixKey, Any.pack(keystate.convertTo(Format.PROTOBUF)), timeout);
        ghost.bind(eventCoordinatesKey(keystate), Any.pack(Boxed.newBuilder().setS(prefixKey).build()), timeout);
    }

    private String eventCoordinatesKey(KeyState keystate) {
        return qb64(DigestAlgorithm.DEFAULT.digest(keystate.getCoordinates().toByteString()));
    }

    @Override
    public Optional<Bound> lookup(Identifier prefix, Duration timeout) throws TimeoutException {
        if (prefix.isTransferable()) {
            throw new IllegalArgumentException("Identifier must be non-transferrable: " + prefix);
        }
        String key = qb64(prefix);
        return ghost.lookup(key, timeout).stream().map(v -> decode(key, v)).findFirst();
    }

    @Override
    public Optional<KeyState> resolve(EventCoordinates coordinates, Duration timeout) throws TimeoutException {
        return null;
    }

    @Override
    public Optional<KeyState> resolve(Identifier prefix, Duration timeout) throws TimeoutException {
        if (!prefix.isTransferable()) {
            throw new IllegalArgumentException("Identifier must be transferrable: " + prefix);
        }
        String key = qb64(prefix);
        return ghost.lookup(key, timeout).stream().map(v -> decodeKeyState(key, v)).findFirst();
    }

    private Bound decode(String key, Any value) {
        if (value.is(Bound.class)) {
            try {
                return value.unpack(Bound.class);
            } catch (InvalidProtocolBufferException e) {
                log.info("Unable to deserialize bound value of: {}", key);
            }
        }
        return null;
    }

    private KeyState decodeKeyState(String key, Any value) {
        if (value.is(com.salesfoce.apollo.stereotomy.event.proto.KeyState.class)) {
            try {
                return new KeyStateImpl(value.unpack(com.salesfoce.apollo.stereotomy.event.proto.KeyState.class));
            } catch (InvalidProtocolBufferException e) {
                log.info("Unable to deserialize key state value of: {}", key);
            }
        }
        return null;
    }
}
