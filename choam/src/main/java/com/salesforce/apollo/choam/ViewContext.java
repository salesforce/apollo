/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.cryptography.*;
import com.salesforce.apollo.membership.Member;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.salesforce.apollo.cryptography.QualifiedBase64.publicKey;

/**
 * @author hal.hildebrand
 */
public class ViewContext {

    private final static Logger                    log = LoggerFactory.getLogger(ViewContext.class);
    private final        BlockProducer             blockProducer;
    private final        Context<Member>           context;
    private final        Parameters                params;
    private final        Map<Digest, Short>        roster;
    private final        Signer                    signer;
    private final        Map<Member, Verifier>     validators;
    private final        Supplier<Context<Member>> pendingView;

    public ViewContext(Context<Member> context, Parameters params, Supplier<Context<Member>> pendingView, Signer signer,
                       Map<Member, Verifier> validators, BlockProducer blockProducer) {
        this.blockProducer = blockProducer;
        this.context = context;
        this.roster = new HashMap<>();
        this.params = params;
        this.signer = signer;
        this.validators = validators;
        this.pendingView = pendingView;

        var remapped = CHOAM.rosterMap(params.context(), context.allMembers().toList());
        short pid = 0;
        for (Digest d : remapped.keySet().stream().sorted().toList()) {
            roster.put(remapped.get(d).getId(), pid++);
        }
    }

    public static String print(Certification c, DigestAlgorithm algo) {
        return String.format("id: %s sig: %s", Digest.from(c.getId()), algo.digest(c.getSignature().toByteString()));
    }

    public static String print(Validate v, DigestAlgorithm algo) {
        return String.format("id: %s hash: %s sig: %s", Digest.from(v.getWitness().getId()), Digest.from(v.getHash()),
                             algo.digest(v.getWitness().getSignature().toByteString()));
    }

    public static String print(SignedViewMember svm, DigestAlgorithm algo) {
        return print(svm.getVm(), algo);
    }

    public static String print(ViewMember vm, DigestAlgorithm algo) {
        return String.format("id: %s vid: %s key: %s sig: %s", Digest.from(vm.getId()), Digest.from(vm.getView()),
                             algo.digest(publicKey(vm.getConsensusKey()).getEncoded()),
                             algo.digest(vm.getSignature().toByteString()));
    }

    public Block checkpoint() {
        return blockProducer.checkpoint();
    }

    public Context<Member> context() {
        return context;
    }

    public Validate generateValidation(HashedBlock block) {
        log.trace("Signing: {} block: {} height: {} on: {}", block.block.getBodyCase(), block.hash, block.height(),
                  params.member().getId());
        JohnHancock signature = signer.sign(block.block.getHeader().toByteString());
        if (signature == null) {
            log.error("Unable to sign: {} block: {} height: {} on: {}", block.block.getBodyCase(), block.hash,
                      block.height(), params.member().getId());
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

    public Validate generateValidation(SignedViewMember svm) {
        JohnHancock signature = signer.sign(svm.getVm().toByteString());
        if (signature == null) {
            log.error("Unable to sign view member: {} on: {}", print(svm, params.digestAlgorithm()),
                      params.member().getId());
            return null;
        }
        if (log.isTraceEnabled()) {
            log.trace("Signed view member: {} with sig: {} on: {}", print(svm, params.digestAlgorithm()),
                      params().digestAlgorithm().digest(signature.toSig().toByteString()), params.member().getId());
        }
        var validation = Validate.newBuilder()
                                 .setHash(svm.getVm().getId())
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

    /**
     * The process has failed
     */
    public void onFailure() {
        blockProducer.onFailure();
    }

    public Parameters params() {
        return params;
    }

    public Context<Member> pendingView() {
        return pendingView.get();
    }

    public Block produce(ULong l, Digest hash, Assemble assemble, HashedBlock checkpoint) {
        return blockProducer.produce(l, hash, assemble, checkpoint);
    }

    public Block produce(ULong l, Digest hash, Executions executions, HashedBlock checkpoint) {
        return blockProducer.produce(l, hash, executions, checkpoint);
    }

    public void publish(HashedCertifiedBlock block) {
        blockProducer.publish(block.hash, block.certifiedBlock);
    }

    public Block reconfigure(Map<Member, Join> aggregate, Digest nextViewId, HashedBlock lastBlock,
                             HashedBlock checkpoint) {
        return blockProducer.reconfigure(aggregate, nextViewId, lastBlock, checkpoint);
    }

    public Map<Digest, Short> roster() {
        return roster;
    }

    public boolean validate(HashedBlock block, Validate validate) {
        Verifier v = verifierOf(validate);
        if (v == null) {
            log.debug("no validation witness: {} for: {} block: {} on: {}", Digest.from(validate.getWitness().getId()),
                      block.block.getBodyCase(), block.hash, params.member().getId());
            return false;
        }
        return v.verify(JohnHancock.from(validate.getWitness().getSignature()), block.block.getHeader().toByteString());
    }

    public boolean validate(SignedViewMember svm, Validate validate) {
        Verifier v = verifierOf(validate);
        if (v == null) {
            return false;
        }
        final var valid = v.verify(JohnHancock.from(validate.getWitness().getSignature()), svm.getVm().toByteString());
        if (!valid) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to validate view member: {} from validation: [{}] key: {} on: {}",
                          print(svm, params.digestAlgorithm()), print(validate, params.digestAlgorithm()),
                          params.digestAlgorithm().digest(v.toString()), params.member().getId());
            }
        }
        return valid;
    }

    protected Verifier verifierOf(Validate validate) {
        final var mid = Digest.from(validate.getWitness().getId());
        var m = context.getMember(mid);
        if (m == null) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to validate key by non existent validator: [{}] on: {}",
                          print(validate, params.digestAlgorithm()), params.member().getId());
            }
            return null;
        }
        Verifier v = validators.get(m);
        if (v == null) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to validate key by non existent validator: [{}] on: {}",
                          print(validate, params.digestAlgorithm()), params.member().getId());
            }
            return null;
        }
        return v;
    }
}
