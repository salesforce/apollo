/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;

import io.grpc.Status;

/**
 * @author hal.hildebrand
 *
 */
public interface Committee {

    static Map<Member, Verifier> validatorsOf(Reconfigure reconfigure, Context<Member> context) {
        return reconfigure.getViewList()
                          .stream()
                          .collect(Collectors.toMap(e -> context.getMember(new Digest(e.getId())),
                                                    e -> new DefaultVerifier(publicKey(e.getConsensusKey()))));
    }

    /**
     * Create a view based on the cut of the supplied hash across the rings of the
     * base context
     */
    static Context<Member> viewFor(Digest hash, Context<? super Member> baseContext) {
        Context<Member> newView = new ContextImpl<>(hash, baseContext.getProbabilityByzantine(),
                                                    baseContext.getRingCount(), baseContext.getBias());
        Set<Member> successors = viewMembersOf(hash, baseContext);
        successors.forEach(e -> {
            if (baseContext.isActive(e)) {
                newView.activate(e);
            } else {
                newView.offline(e);
            }
        });
        return newView;
    }

    static Set<Member> viewMembersOf(Digest hash, Context<? super Member> baseContext) {
        Set<Member> successors = new HashSet<>();
        baseContext.successors(hash, m -> {
            if (successors.size() == baseContext.getRingCount()) {
                return false;
            }
            if (baseContext.isOffline(m.getId())) {
                return false;
            }
            return successors.add(m);
        });
        return successors;
    }

    void accept(HashedCertifiedBlock next);

    default void assembled() {
    }

    void complete();

    boolean isMember();

    ViewMember join(JoinRequest request, Digest from);

    Logger log();

    Parameters params();

    default void regenerate() {
        throw new IllegalStateException("Should not be called on this implementation");
    }

    default SubmitResult submit(SubmitTransaction request) {
        log().trace("Cannot submit txn, inactive committee");
        return SubmitResult.newBuilder().setSuccess(false).setStatus("Cannot submit txn, inactive committee").build();
    }

    default ListenableFuture<Status> submitTxn(Transaction transaction) {
        log().trace("Cannot submit txn, inactive committee");
        SettableFuture<Status> f = SettableFuture.create();
        f.set(Status.UNAVAILABLE.withDescription("Cannot submit txn, inactive committee on: " + params().member()));
        return f;
    }

    boolean validate(HashedCertifiedBlock hb);

    default boolean validate(HashedCertifiedBlock hb, Certification c, Map<Member, Verifier> validators) {
        Parameters params = params();
        Digest wid = new Digest(c.getId());
        var witness = params.context().getMember(wid);
        if (witness == null) {
            log().debug("Witness does not exist: {} in: {} validating: {} on: {}", wid, params.context().getId(), hb,
                        params.member());
            return false;
        }
        var verify = validators.get(witness);
        if (verify == null) {
            log().debug("Witness: {} is not a validator for: {} validating: {} on: {}", wid, params.context().getId(),
                        hb, params.member());
            return false;
        }

        final boolean verified = verify.verify(new JohnHancock(c.getSignature()), hb.block.getHeader().toByteString());
        if (!verified) {
            log().debug("Failed verification: {} using: {} key: {} on: {}", verified, witness.getId(),
                        DigestAlgorithm.DEFAULT.digest(verify.toString()), params.member());
        } else {
            log().trace("Verified: {} using: {} key: {} on: {}", verified, witness,
                        DigestAlgorithm.DEFAULT.digest(verify.toString()), params.member());
        }
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
                log().debug("Failed to validate: {} height: {} by: {} on: {}}", hb.hash, hb.height(),
                            new Digest(w.getId()), params.member());
            } else {
                valid++;
            }
        }
        final int toleranceLevel = params.toleranceLevel();
        log().trace("Validate: {} height: {} count: {} needed: {} on: {}}", hb.hash, hb.height(), valid,
                    toleranceLevel + 1, params.member());
        return valid > toleranceLevel;
    }

    default boolean validateRegeneration(HashedCertifiedBlock hb) {
        if (!hb.block.hasGenesis()) {
            return false;
        }
        var reconfigure = hb.block.getGenesis().getInitialView();
        return validate(hb, validatorsOf(reconfigure, params().context()));
    }
}
