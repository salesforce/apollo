/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.support.HashedBlock.hash;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
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
    static final Logger log = LoggerFactory.getLogger(Committee.class);

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

    void complete();

    boolean isMember();

    ViewMember join(JoinRequest request, Digest from);

    Parameters params();

    default void regenerate() {
        throw new IllegalStateException("Should not be called on this implementation");
    }

    default boolean validate(byte[] headerHash, Certification c, Map<Member, Verifier> validators) {
        Parameters params = params();
        Digest wid = new Digest(c.getId());
        var witness = params.context().getMember(wid);
        if (witness == null) {
            log.trace("Witness does not exist: {} in: {} validating: {} on: {}", wid, params.context().getId(),
                      headerHash, params.member());
            return false;
        }
        var verify = validators.get(witness);
        if (verify == null) {
            log.trace("Witness: {} is not a validator for: {} validating: {} on: {}", wid, params.context().getId(),
                      headerHash, params.member());
            return false;
        }

        final boolean verified = verify.verify(new JohnHancock(c.getSignature()), headerHash);
        log.trace("Verified: {} using: {} key: {} on: {}", verified, witness,
                  DigestAlgorithm.DEFAULT.digest(verify.getPublicKey().getEncoded()), params.member());
        return verified;
    }

    boolean validate(HashedCertifiedBlock hb);

    default boolean validate(HashedCertifiedBlock hb, Map<Member, Verifier> validators) {
        Parameters params = params();
        byte[] headerHash = hash(hb.block.getHeader(), params.digestAlgorithm()).getBytes();
        log.trace("Validating block: {} height: {} certs: {} on: {}", hb.hash, hb.height(),
                  hb.certifiedBlock.getCertificationsList().stream().map(c -> new Digest(c.getId())).toList(),
                  params.member());
        int valid = 0;
        for (var w : hb.certifiedBlock.getCertificationsList()) {
            if (!validate(headerHash, w, validators)) {
                log.error("Failed to validate: {} height: {} by: {} on: {}}", hb.hash, hb.height(),
                          new Digest(w.getId()), params.member());
            } else {
                valid++;
            }
        }
        final int toleranceLevel = params.context().toleranceLevel();
        log.trace("Validate: {} height: {} count: {} needed: {} on: {}}", hb.hash, hb.height(), valid, toleranceLevel,
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
