/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.digestOf;

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

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.DelegatedKERL;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class Maat extends DelegatedKERL {
    private static Logger log = LoggerFactory.getLogger(Maat.class);

    private final Context<Member> context;

    private final KERL validators;

    public Maat(Context<Member> context, KERL delegate, KERL validators) {
        super(delegate);
        this.context = context;
        this.validators = validators;
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
        final List<KeyEvent> filtered = events.stream().filter(e -> {
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
        }).toList();
        return filtered.isEmpty() && attachments.isEmpty() ? emptyFutureList() : super.append(filtered, attachments);
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
        final Context<Member> ctx = context;
        var successors = Context.uniqueSuccessors(ctx, digestOf(event.getIdentifier().toIdent(), digest.getAlgorithm()))
                                .stream()
                                .map(m -> m.getId())
                                .collect(Collectors.toSet());

        record validator(EstablishmentEvent validating, JohnHancock signature) {}
        var mapped = new CopyOnWriteArrayList<validator>();
        final var serialized = event.toKeyEvent_().toByteString();

        return delegate.getValidations(event.getCoordinates()).thenCompose(validations -> {
            var futures = validations.entrySet().stream().map(e -> validators.getKeyEvent(e.getKey()).thenApply(ev -> {
                if (ev == null) {
                    return null;
                }
                var signer = (EstablishmentEvent) ev;
                if ((signer.getIdentifier() instanceof SelfAddressingIdentifier sai)) {
                    if (!successors.contains(sai.getDigest())) {
                        log.warn("Signature: {} not successor of: {} ", signer.getCoordinates(),
                                 event.getCoordinates());
                    }
                    mapped.add(new validator(signer, e.getValue()));
                    log.trace("Signature: {} valid for: {}", signer.getCoordinates(), event.getCoordinates());
                } else {
                    log.warn("Signature not SAI: {} for: {}", signer.getCoordinates(), event.getCoordinates(),
                             event.getCoordinates());
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

                var verified = 0;
                for (var r : mapped) {
                    var verifier = new DefaultVerifier(r.validating.getKeys().get(0));
                    if (verifier.verify(r.signature, serialized)) {
                        verified++;
                    } else {
                        log.trace("Cannot verify sig: {} of: {} by: {}", r.signature, event.getCoordinates(),
                                  r.validating.getIdentifier());
                    }
                }
                var validated = verified >= context.majority();

                log.trace("Validated: {} valid: {} out of: {} required: {} for: {}  ", validated, verified,
                          mapped.size(), ctx.majority(), event.getCoordinates());
                return validated;
            });
        });
    }

    private CompletableFuture<List<KeyState>> emptyFutureList() {
        var fs = new CompletableFuture<List<KeyState>>();
        fs.complete(Collections.emptyList());
        return fs;
    }

}
