/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.util.HashMap;
import java.util.Map;

import org.joou.ULong;
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
import com.salesforce.apollo.crypto.DigestAlgorithm;
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

    public static String print(Validate v, DigestAlgorithm algo) {
        return String.format("id: %s hash: %s sig: %s", Digest.from(v.getWitness().getId()), Digest.from(v.getHash()),
                             algo.digest(v.getWitness().getSignature().toByteString()));
    }

    public static String print(ViewMember vm, DigestAlgorithm algo) {
        return String.format("id: %s key: %s sig: %s", Digest.from(vm.getId()),
                             algo.digest(publicKey(vm.getConsensusKey()).getEncoded()),
                             algo.digest(vm.getSignature().toByteString()));
    }

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
        var validation = Validate.newBuilder()
                                 .setHash(block.hash.toDigeste())
                                 .setWitness(Certification.newBuilder()
                                                          .setId(params.member().getId().toDigeste())
                                                          .setSignature(signature.toSig())
                                                          .build())
                                 .build();
        return validation;
    }

    public Validate generateValidation(ViewMember vm) {
        JohnHancock signature = signer.sign(vm.getSignature().toByteString());
        if (signature == null) {
            log.error("Unable to sign view member: {} on: {}", print(vm, params.digestAlgorithm()), params.member());
            return null;
        }
        if (log.isTraceEnabled()) {
            log.trace("Signed view member: {} with sig: {} on: {}", print(vm, params.digestAlgorithm()),
                      params().digestAlgorithm().digest(signature.toSig().toByteString()), params.member());
        }
        var validation = Validate.newBuilder()
                                 .setHash(vm.getId())
                                 .setWitness(Certification.newBuilder()
                                                          .setId(params.member().getId().toDigeste())
                                                          .setSignature(signature.toSig())
                                                          .build())
                                 .build();
        return validation;
    }

    public Block genesis(Map<Member, Join> slate, Digest nextViewId, HashedBlock previous) {
        return blockProducer.genesis(slate, nextViewId, previous);
    }

    public Signer getSigner() {
        return signer;
    }

    public Parameters params() {
        return params;
    }

    public Block produce(ULong l, Digest hash, Assemble assemble, HashedBlock checkpoint) {
        return blockProducer.produce(l, hash, assemble, checkpoint);
    }

    public Block produce(ULong l, Digest hash, Executions executions, HashedBlock checkpoint) {
        return blockProducer.produce(l, hash, executions, checkpoint);
    }

    public void publish(HashedCertifiedBlock block) {
        blockProducer.publish(block.certifiedBlock);
    }

    public Block reconfigure(Map<Member, Join> aggregate, Digest nextViewId, HashedBlock hashedBlock,
                             HashedBlock checkpoint) {
        return blockProducer.reconfigure(aggregate, nextViewId, hashedBlock, checkpoint);
    }

    public Map<Digest, Short> roster() {
        return roster;
    }

    public boolean validate(HashedBlock block, Validate validate) {
        Verifier v = verifierOf(validate);
        return v == null ? false : v.verify(JohnHancock.from(validate.getWitness().getSignature()),
                                            block.block.getHeader().toByteString());
    }

    public boolean validate(ViewMember vm, Validate validate) {
        Verifier v = verifierOf(validate);
        if (v == null) {
            return false;
        }
        final var valid = v.verify(JohnHancock.from(validate.getWitness().getSignature()),
                                   vm.getSignature().toByteString());
        if (!valid) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to validate view member: {} from validation: {} key: {} on: {}",
                          print(vm, params.digestAlgorithm()), print(validate, params.digestAlgorithm()),
                          params.digestAlgorithm().digest(v.toString()), params.member());
            }
        }
        return valid;
    }

    protected Verifier verifierOf(Validate validate) {
        final var mid = Digest.from(validate.getWitness().getId());
        var m = context.getMember(mid);
        if (m == null) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to validate key by non existant validator: {} on: {}",
                          print(validate, params.digestAlgorithm()), params.member());
            }
            return null;
        }
        Verifier v = validators.get(m);
        if (v == null) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to validate key by non existant validator: {} on: {}",
                          print(validate, params.digestAlgorithm()), params.member());
            }
            return null;
        }
        return v;
    }
}
