/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.security.PublicKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.DelegatedKERL;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * @author hal.hildebrand
 *
 */
public class Maat extends DelegatedKERL {
    private static Logger log = LoggerFactory.getLogger(Maat.class);

    private final Context<Member> context;

    public Maat(Context<Member> context, KERL delegate) {
        super(delegate);
        this.context = context;
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        return append(Collections.singletonList(event),
                      Collections.emptyList()).thenApply(l -> l.isEmpty() ? null : l.get(0));
    }

    @Override
    public CompletableFuture<List<KeyState>> append(KeyEvent... events) {
        return append(Arrays.asList(events), Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        return super.append(events.stream().filter(e -> {
            if (e instanceof EstablishmentEvent est &&
                est.getCoordinates().getSequenceNumber().equals(ULong.valueOf(0))) {
                try {
                    return validate(est).get();
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e1) {
                    log.error("error validating: {}", est.getCoordinates(), e1.getCause());
                    return false;
                }
            }
            return true;
        }).toList(), attachments);
    }

    public CompletableFuture<Boolean> validate(EstablishmentEvent event) {
        Digest digest;
        if (event.getIdentifier() instanceof SelfAddressingIdentifier said) {
            digest = said.getDigest();
        } else {
            final CompletableFuture<Boolean> fs = new CompletableFuture<Boolean>();
            fs.complete(false);
            return fs;
        }
        var successors = context.successors(digest).stream().map(m -> m.getId()).collect(Collectors.toSet());

        record validator(EstablishmentEvent validating, JohnHancock signature) {}
        var mapped = new CopyOnWriteArrayList<validator>();
        final ByteString serialized = event.toKeyEvent_().toByteString();

        return delegate.getValidations(event.getCoordinates()).thenCompose(validations -> {
            var futures = validations.entrySet().stream().map(e -> delegate.getKeyEvent(e.getKey()).thenApply(ev -> {
                if (ev == null) {
                    return null;
                }
                var evnt = (EstablishmentEvent) ev;
                if ((evnt.getIdentifier() instanceof SelfAddressingIdentifier sai &&
                     successors.contains(sai.getDigest()))) {
                    mapped.add(new validator(evnt, e.getValue()));
                }
                return event;
            })).toList();
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(e -> {
                log.trace("Evaluating validation of: {} validations: {} mapped: {}", event.getCoordinates(),
                          validations.size(), mapped.size());
                if (mapped.size() == 0) {
                    log.warn("No validations of: {} ", event.getCoordinates());
                    return false;
                }
                var validating = new PublicKey[mapped.size()];
                byte[][] signatures = new byte[mapped.size()][];

                int index = 0;
                for (var r : mapped) {
                    validating[index] = r.validating.getKeys().get(0);
                    signatures[index++] = r.signature.getBytes()[0];
                }

                var algo = SignatureAlgorithm.lookup(validating[0]);
                var validated = new JohnHancock(algo,
                                                signatures).verify(SigningThreshold.unweighted(context.majority()),
                                                                   validating,
                                                                   BbBackedInputStream.aggregate(serialized));
                log.trace("Validated: {} for: {}  ", validated, event.getCoordinates());
                return validated;
            });
        });
    }

}
