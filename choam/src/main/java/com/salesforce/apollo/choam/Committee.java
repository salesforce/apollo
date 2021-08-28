/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.google.common.collect.Sets;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.Endorsements;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Outcome;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.ServiceUnavailable;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Committee {

    static Map<Member, Verifier> validators(Map<Member, ViewMember> validators) {
        return validators.entrySet().stream()
                         .collect(Collectors.toMap(e -> e.getKey(),
                                                   e -> new DefaultVerifier(publicKey(e.getValue()
                                                                                       .getConsensusKey()))));
    }

    static Map<Member, Verifier> validatorsOf(Reconfigure reconfigure, Context<Member> context) {
        return reconfigure.getViewList().stream()
                          .collect(Collectors.toMap(e -> context.getMember(new Digest(e.getId())),
                                                    e -> new DefaultVerifier(publicKey(e.getConsensusKey()))));
    }

    /**
     * Create a view based on the cut of the supplied hash across the rings of the
     * base context
     */
    static Context<Member> viewFor(Digest hash, Context<? super Member> baseContext) {
        Context<Member> newView = new Context<>(hash, 0.33, baseContext.getRingCount());
        Set<Member> successors = viewMembersOf(hash, baseContext);
        successors.forEach(e -> {
            if (baseContext.isActive(e)) {
                newView.activate(e);
            } else {
                newView.offline(e);
            }
        });
        assert newView.getActive().size() + newView.getOffline().size() == baseContext.getRingCount();
        return newView;
    }

    static Set<Member> viewMembersOf(Digest hash, Context<? super Member> baseContext) {
        Set<Member> successors = new HashSet<>();
        baseContext.successors(hash, m -> {
            if (successors.size() == baseContext.getRingCount()) {
                return false;
            }
            boolean contained = successors.contains(m);
            successors.add(m);
            return !contained;
        });
        assert successors.size() == baseContext.getRingCount();
        return successors;
    }

    void accept(HashedCertifiedBlock next);

    default void assemble(Digest nextViewId) {
        throw new IllegalStateException("Should not be assembling for next view: " + nextViewId + " on: "
        + params().member());
    }

    void complete();

    default void endorsements(Endorsements endorsements) {
        log().trace("Not processing endorsement, not a committee member on: {}", params().member());
    }

    Logger log();

    boolean isMember();

    default void join(Join join) {
        log().trace("Not processing join, not a committee member on: {}", params().member());
    }

    ViewMember join(JoinRequest request, Digest from);

    default Certification join2(JoinRequest request, Digest from) {
        log().debug("Cannot process join request from: {}, not a committee member on: {}", from, params().member());
        return Certification.getDefaultInstance();
    }

    Parameters params();

    default void regenerate() {
        throw new IllegalStateException("Should not be called on this implementation");
    }

    default SubmitResult submit(SubmitTransaction request) {
        log().debug("Cannot submit txn inactive committee on: {}", params().member());
        return SubmitResult.newBuilder().setOutcome(Outcome.INACTIVE_COMMITTEE).build();
    }

    default void submitTxn(Transaction transaction, CompletableFuture<Boolean> result) {
        log().debug("Cannot submit txn inactive committee on: {}", params().member());
        result.completeExceptionally(new ServiceUnavailable());
    }

    boolean validate(HashedCertifiedBlock hb);

    default boolean validate(HashedCertifiedBlock hb, Certification c, Map<Member, Verifier> validators) {
        Parameters params = params();
        Digest wid = new Digest(c.getId());
        var witness = params.context().getMember(wid);
        if (witness == null) {
            log().trace("Witness does not exist: {} in: {} validating: {} on: {}", wid, params.context().getId(), hb,
                        params.member());
            return false;
        }
        var verify = validators.get(witness);
        if (verify == null) {
            log().trace("Witness: {} is not a validator for: {} validating: {} on: {}", wid, params.context().getId(),
                        hb, params.member());
            return false;
        }

        final boolean verified = verify.verify(new JohnHancock(c.getSignature()), hb.block.getHeader().toByteString());
        log().trace("Verified: {} using: {} key: {} on: {}", verified, witness,
                    DigestAlgorithm.DEFAULT.digest(verify.getPublicKey().getEncoded()), params.member());
        return verified;
    }

    default boolean validate(HashedCertifiedBlock hb, Map<Member, Verifier> validators) {
        Parameters params = params();

        log().trace("Validating block: {} height: {} certs: {} on: {}", hb.hash, hb.height(),
                    hb.certifiedBlock.getCertificationsList().stream().map(c -> new Digest(c.getId())).toList(),
                    params.member());
        int valid = 0;
        for (var w : hb.certifiedBlock.getCertificationsList()) {
            if (!validate(hb, w, validators)) {
                log().error("Failed to validate: {} height: {} by: {} on: {}}", hb.hash, hb.height(),
                            new Digest(w.getId()), params.member());
            } else {
                valid++;
            }
        }
        final int toleranceLevel = params.context().toleranceLevel();
        log().trace("Validate: {} height: {} count: {} needed: {} on: {}}", hb.hash, hb.height(), valid, toleranceLevel,
                    params.member());
        return valid > toleranceLevel;
    }

    default boolean validateRegeneration(HashedCertifiedBlock hb) {
        if (!hb.block.hasGenesis()) {
            return false;
        }
        var reconfigure = hb.block.getGenesis().getInitialView();
        var validators = validatorsOf(reconfigure, params().context());
        ArrayList<Member> diff = new ArrayList<>(Sets.difference(validators.keySet(),
                                                                 viewMembersOf(new Digest(reconfigure.getId()),
                                                                               params().context())));
        diff.forEach(m -> validators.remove(m));
        return validate(hb, validators);
    }
}
