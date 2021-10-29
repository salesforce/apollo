/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.choam.proto.Assemble;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.choam.support.HashedBlock;
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

    private final BlockProducer         blockProducer;
    private final Context<Member>       context;
    private final Parameters            params;
    private final Map<Digest, Short>    roster;
    private final Signer                signer;
    private final Map<Member, Verifier> validators;

    public ViewContext(Context<Member> context, Parameters params, Signer signer, Map<Member, Verifier> validators,
                       BlockProducer blockProducer) {
        this.blockProducer = blockProducer;
        this.context = context;
        this.roster = new HashMap<>();
        this.params = params;
        this.signer = signer;
        this.validators = validators;

        var remapped = CHOAM.rosterMap(params.context(), context.activeMembers());
        short pid = 0;
        for (Digest d : remapped.keySet().stream().sorted().toList()) {
            roster.put(remapped.get(d).getId(), pid++);
        }
    }

    public Block checkpoint() {
        return blockProducer.checkpoint();
    }

    public Context<Member> context() {
        return context;
    }

    public Validate generateValidation(HashedBlock block) {
        log.trace("Signing block: {} height: {} on: {}", block.hash, block.height(), params.member());
        JohnHancock signature = signer.sign(block.block.getHeader().toByteString());
        if (signature == null) {
            log.error("Unable to sign block: {} height: {} on: {}", block.hash, block.height(), params.member());
            return null;
        }
        var validation = Validate.newBuilder().setHash(block.hash.toDigeste())
                                 .setWitness(Certification.newBuilder().setId(params.member().getId().toDigeste())
                                                          .setSignature(signature.toSig()).build())
                                 .build();
        return validation;
    }

    public Validate generateValidation(ViewMember vm) {
        final var vmId = Digest.from(vm.getId());
        log.trace("Signing view member: {}  on: {}", vmId, params.member());
        final var vmSig = vm.getSignature();
        JohnHancock signature = signer.sign(vmSig.toByteString());
        if (signature == null) {
            log.error("Unable to sign view member: {}  on: {}", vmId, params.member());
            return null;
        }
        var validation = Validate.newBuilder().setHash(vm.getId())
                                 .setWitness(Certification.newBuilder().setId(params.member().getId().toDigeste())
                                                          .setSignature(signature.toSig()).build())
                                 .build();
        return validation;
    }

    public Signer getSigner() {
        return signer;
    }

    public Parameters params() {
        return params;
    }

    public Block produce(long l, Digest hash, Assemble assemble) {
        return blockProducer.produce(l, hash, assemble);
    }

    public Block produce(long l, Digest hash, Executions executions) {
        return blockProducer.produce(l, hash, executions);
    }

    public void publish(HashedCertifiedBlock block) {
        blockProducer.publish(block.certifiedBlock);
    }

    public Block reconfigure(Map<Member, Join> aggregate, Digest nextViewId, HashedBlock hashedBlock) {
        return blockProducer.reconfigure(aggregate, nextViewId, hashedBlock);
    }

    public Map<Digest, Short> roster() {
        return roster;
    }

    public boolean validate(ViewMember vm, Validate validate) {
        final var mid = Digest.from(validate.getWitness().getId());
        var m = context.getMember(mid);
        if (m == null) {
            log.debug("Unable to validate key by non existant validator: {} on: {}", mid, params.member());
            return false;
        }
        Verifier v = validators.get(m);
        if (v == null) {
            log.debug("Unable to validate key by non existant validator: {} on: {}", mid, params.member());
            return false;
        }
        return v.verify(JohnHancock.from(validate.getWitness().getSignature()), vm.getSignature().toByteString());
    }
}
