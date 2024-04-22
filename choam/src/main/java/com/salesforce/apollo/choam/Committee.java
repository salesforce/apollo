/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.salesforce.apollo.choam.proto.Certification;
import com.salesforce.apollo.choam.proto.Reconfigure;
import com.salesforce.apollo.choam.proto.SubmitResult;
import com.salesforce.apollo.choam.proto.SubmitResult.Result;
import com.salesforce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.StaticContext;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.cryptography.Verifier.DefaultVerifier;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.MockMember;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.salesforce.apollo.cryptography.QualifiedBase64.publicKey;

/**
 * @author hal.hildebrand
 */
public interface Committee {

    static Map<Member, Verifier> validatorsOf(Reconfigure reconfigure, Context<Member> context, Digest member,
                                              Logger log) {
        var validators = reconfigure.getJoinsList().stream().collect(Collectors.toMap(e -> {
            var id = new Digest(e.getMember().getVm().getId());
            var m = context.getMember(id);
            if (m == null) {
                log.info("No member for validator: {}, returning mock on: {}", id, member);
                return new MockMember(id);
            } else {
                return m;
            }
        }, e -> {
            var vm = e.getMember().getVm();
            if (vm.hasConsensusKey()) {

                return new DefaultVerifier(publicKey(vm.getConsensusKey()));
            } else {
                log.info("No member for validator: {}, returning mock on: {}", Digest.from(vm.getId()), member);
                return Verifier.NO_VERIFIER;
            }
        }));
        assert !validators.isEmpty() : "No validators in this reconfiguration of: " + context.getId();
        return validators;
    }

    /**
     * Create a view based on the cut of the supplied hash across the rings of the base context
     */
    static Context<Member> viewFor(Digest hash, Context<? super Member> baseContext) {
        Set<Member> successors = viewMembersOf(hash, baseContext);
        var newView = new StaticContext<>(hash, baseContext.getProbabilityByzantine(), 3, successors,
                                          baseContext.getEpsilon(), successors.size());
        return newView;
    }

    static Set<Member> viewMembersOf(Digest hash, Context<? super Member> baseContext) {
        Set<Member> successors = new HashSet<>();
        baseContext.successors(hash, m -> {
            if (successors.size() == baseContext.getRingCount()) {
                return false;
            }
            return successors.add(m);
        });
        return successors;
    }

    void accept(HashedCertifiedBlock next);

    void complete();

    boolean isMember();

    Logger log();

    void nextView(Digest diadem, Context<Member> pendingView);

    Parameters params();

    default void regenerate() {
        throw new IllegalStateException("Should not be called on this implementation");
    }

    default SubmitResult submit(Transaction request) {
        log().debug("Cannot submit txn, inactive committee: {} on: {}", getClass().getSimpleName(),
                    params().member().getId());
        return SubmitResult.newBuilder().setResult(Result.INACTIVE).build();
    }

    default SubmitResult submitTxn(Transaction transaction) {
        log().debug("Cannot process txn, inactive committee: {} on: {}", getClass().getSimpleName(),
                    params().member().getId());
        return SubmitResult.newBuilder().setResult(Result.UNAVAILABLE).build();
    }

    boolean validate(HashedCertifiedBlock hb);

    default boolean validate(HashedCertifiedBlock hb, Certification c, Map<Member, Verifier> validators) {
        Parameters params = params();
        Digest wid = new Digest(c.getId());
        var witness = params.context().getMember(wid);
        if (witness == null) {
            log().debug("Witness does not exist: {} in: {} validating: {} on: {}", wid, params.context().getId(), hb,
                        params.member().getId());
            return false;
        }
        var verify = validators.get(witness);
        if (verify == null) {
            log().debug("Witness: {} is not a validator for: {} validating: {} on: {}", wid, params.context().getId(),
                        hb, params.member().getId());
            return false;
        }

        final boolean verified = verify.verify(new JohnHancock(c.getSignature()), hb.block.getHeader().toByteString());
        if (!verified) {
            log().debug("Failed verification: {} hash: {} height: {} using: {} : {} on: {}", hb.block.getBodyCase(),
                        hb.hash, hb.height(), witness.getId(), verify, params.member().getId());
        } else if (log().isTraceEnabled()) {
            log().trace("Verified: {} hash: {} height: {} using: {} : {} on: {}", hb.block.getBodyCase(), hb.hash,
                        hb.height(), witness.getId(), verify, params.member().getId());
        }
        return verified;
    }

    default boolean validate(HashedCertifiedBlock hb, Map<Member, Verifier> validators) {
        Parameters params = params();
        log().trace("Validating block: {} hash: {} height: {} certs: {} on: {}", hb.block.getBodyCase(), hb.hash,
                    hb.height(),
                    hb.certifiedBlock.getCertificationsList().stream().map(c -> new Digest(c.getId())).toList(),
                    params.member().getId());
        int valid = 0;
        for (var w : hb.certifiedBlock.getCertificationsList()) {
            if (!validate(hb, w, validators)) {
                log().debug("Failed to validate: {} height: {} by: {} on: {}}", hb.hash, hb.height(),
                            new Digest(w.getId()), params.member().getId());
            } else {
                valid++;
            }
        }
        final int toleranceLevel = params.context().toleranceLevel();
        log().trace("Validate: {} height: {} count: {} needed: {} on: {}}", hb.hash, hb.height(), valid, toleranceLevel,
                    params.member().getId());
        return valid > toleranceLevel;
    }

    default boolean validateRegeneration(HashedCertifiedBlock hb) {
        if (!hb.block.hasGenesis()) {
            return false;
        }
        var reconfigure = hb.block.getGenesis().getInitialView();
        var validators = validatorsOf(reconfigure, params().context(), params().member().getId(), log());
        return !validators.isEmpty() && validate(hb, validators);
    }
}
