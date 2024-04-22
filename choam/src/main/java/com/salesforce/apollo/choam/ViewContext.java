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

    private final static Logger                       log = LoggerFactory.getLogger(ViewContext.class);
    private final        BlockProducer                blockProducer;
    private final        Context<Member>              context;
    private final        Parameters                   params;
    private final        Map<Digest, Short>           roster;
    private final        Signer                       signer;
    private final        Map<Member, Verifier>        validators;
    private final        Supplier<CHOAM.PendingViews> pendingViews;

    public ViewContext(Context<Member> context, Parameters params, Supplier<CHOAM.PendingViews> pendingViews,
                       Signer signer, Map<Member, Verifier> validators, BlockProducer blockProducer) {
        this.blockProducer = blockProducer;
        this.context = context;
        this.roster = new HashMap<>();
        this.params = params;
        this.signer = signer;
        this.validators = validators;
        this.pendingViews = pendingViews;

        var remapped = CHOAM.rosterMap(params.context(), context.allMembers().map(Member::getId).toList());
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
        if (log.isTraceEnabled()) {
            log.trace("Signing: {} block: {} height: {} on: {}", block.block.getBodyCase(), block.hash, block.height(),
                      params.member().getId());
        }
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

    public Block genesis(Map<Digest, Join> slate, Digest nextViewId, HashedBlock previous) {
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

    public CHOAM.PendingViews pendingViews() {
        return pendingViews.get();
    }

    public Block produce(ULong l, Digest hash, Executions executions, HashedBlock checkpoint) {
        return blockProducer.produce(l, hash, executions, checkpoint);
    }

    public void publish(HashedCertifiedBlock block) {
        blockProducer.publish(block.hash, block.certifiedBlock);
    }

    public Block reconfigure(Map<Digest, Join> aggregate, Digest nextViewId, HashedBlock lastBlock,
                             HashedBlock checkpoint) {
        return blockProducer.reconfigure(aggregate, nextViewId, lastBlock, checkpoint);
    }

    public Map<Digest, Short> roster() {
        return roster;
    }

    public JohnHancock sign(SignedViewMember svm) {
        if (log.isTraceEnabled()) {
            log.trace("Signing: {} on: {}", print(svm, params.digestAlgorithm()), params.member().getId());
        }
        return signer.sign(svm.toByteString());
    }

    public JohnHancock sign(Views views) {
        if (log.isTraceEnabled()) {
            log.trace("Signing views: {} on: {}",
                      views.getViewsList().stream().map(v -> Digest.from(v.getDiadem())).toList(),
                      params.member().getId());
        }
        return signer.sign(views.toByteString());
    }

    public boolean validate(HashedBlock block, Validate validate) {
        Verifier v = verifierOf(validate);
        if (v == null) {
            if (log.isDebugEnabled()) {
                log.debug("no validation witness: {} for: {} block: {} on: {}",
                          Digest.from(validate.getWitness().getId()), block.block.getBodyCase(), block.hash,
                          params.member().getId());
            }
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

    public boolean validate(SignedJoin join) {
        Verifier v = verifierOf(join);
        if (v == null) {
            if (log.isDebugEnabled()) {
                log.debug("no verifier: {} for join: {} on: {}", Digest.from(join.getMember()),
                          Digest.from(join.getJoin().getVm().getId()), params.member().getId());
            }
            return false;
        }
        var validated = v.verify(JohnHancock.from(join.getSignature()), join.getJoin().toByteString());
        if (!validated) {
            if (log.isTraceEnabled()) {
                log.trace("Cannot validate view join: [{}] sig: {} signed by: {} on: {}",
                          print(join.getJoin(), params.digestAlgorithm()),
                          params.digestAlgorithm().digest(join.getSignature().toByteString()),
                          Digest.from(join.getMember()), params().member().getId());
            }
        } else if (log.isTraceEnabled()) {
            log.trace("Validated view join: [{}] signed by: {} on: {}", print(join.getJoin(), params.digestAlgorithm()),
                      Digest.from(join.getMember()), params().member().getId());
        }
        return validated;
    }

    public boolean validate(SignedViews sv) {
        Verifier v = verifierOf(sv);
        if (v == null) {
            if (log.isDebugEnabled()) {
                log.debug("no verifier: {} for signed view on: {}", Digest.from(sv.getViews().getMember()),
                          params.member().getId());
            }
            return false;
        }
        var validated = v.verify(JohnHancock.from(sv.getSignature()), sv.getViews().toByteString());
        if (!validated) {
            if (log.isTraceEnabled()) {
                log.trace("Cannot validate views signed by: {} on: {}", Digest.from(sv.getViews().getMember()),
                          params().member().getId());
            }
        } else if (log.isTraceEnabled()) {
            log.trace("Validated views signed by: {} on: {}", Digest.from(sv.getViews().getMember()),
                      params().member().getId());
        }
        return validated;
    }

    protected Verifier verifierOf(Validate validate) {
        return getVerifier(context.getMember(Digest.from(validate.getWitness().getId())));
    }

    protected Verifier verifierOf(SignedJoin sj) {
        return getVerifier(context.getMember(Digest.from(sj.getMember())));
    }

    protected Verifier verifierOf(SignedViews sv) {
        return getVerifier(context.getMember(Digest.from(sv.getViews().getMember())));
    }

    private Verifier getVerifier(Member m) {
        if (m == null) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to get verifier by non existent member: {} on: {}", m.getId(),
                          params.member().getId());
            }
            return null;
        }
        Verifier v = validators.get(m);
        if (v == null) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to validate key by non existent validator: {} on: {}", m.getId(),
                          params.member().getId());
            }
            return null;
        }
        return v;
    }
}
