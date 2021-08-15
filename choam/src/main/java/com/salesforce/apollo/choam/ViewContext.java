/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.support.HashedBlock.hash;
import static com.salesforce.apollo.choam.support.HashedBlock.height;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class ViewContext {
    private final static Logger log = LoggerFactory.getLogger(ViewContext.class);

    private final Context<Member>                context;
    private final Parameters                     params;
    private final Consumer<HashedCertifiedBlock> publisher;
    private final Map<Digest, Short>             roster;
    private final Signer                         signer;
    private final Map<Member, Verifier>          validators;

    public ViewContext(Context<Member> context, Parameters params, Signer signer, Map<Member, Verifier> validators,
                       Consumer<HashedCertifiedBlock> publisher) {
        this.context = context;
        this.roster = new HashMap<>();
        this.params = params;
        this.signer = signer;
        this.validators = validators;
        this.publisher = publisher;

        var remapped = CHOAM.rosterMap(params.context(), context.activeMembers());
        short pid = 0;
        for (Digest d : remapped.keySet().stream().sorted().toList()) {
            roster.put(remapped.get(d).getId(), pid++);
        }
    }

    public Context<Member> context() {
        return context;
    }

    public Validate generateValidation(Digest hash, Block block) {
        byte[] bytes = hash(block.getHeader(), params.digestAlgorithm()).getBytes();
        log.trace("Signing block: {} height: {} on: {}", hash, height(block), params.member());
        JohnHancock signature = signer.sign(bytes);
        if (signature == null) {
            log.error("Unable to sign block: {} height: {} on: {}", hash, height(block), params.member());
            return null;
        }
        var validation = Validate.newBuilder().setHash(hash.toDigeste())
                                 .setWitness(Certification.newBuilder().setId(params.member().getId().toDigeste())
                                                          .setSignature(signature.toSig()).build())
                                 .build();
        return validation;
    }

    public Parameters params() {
        return params;
    }

    public void publish(HashedCertifiedBlock block) {
        publisher.accept(block);
    }

    public Map<Digest, Short> roster() {
        return roster;
    }

    public boolean validate(Member m, PubKey encoded, JohnHancock sig) {
        Verifier v = validators.get(m);
        if (v == null) {
            log.debug("Unable to validate key by non existant validator: {} on: {}", m.getId(), params.member());
            return false;
        }
        return v.verify(sig, encoded.toByteString());
    }
}
