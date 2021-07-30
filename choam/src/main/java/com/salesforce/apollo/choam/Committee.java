/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.support.HashedBlock.hash;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.crypto.Digest;
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

    void accept(HashedCertifiedBlock next);

    HashedBlock getViewChange();

    Parameters params();
 

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

        var signature = new JohnHancock(c.getSignature());
        verify.verify(signature, headerHash);
        return true;
    }

    boolean validate(HashedCertifiedBlock hb);

    default boolean validate(HashedCertifiedBlock hb, Map<Member, Verifier> validators) {
        Parameters params = params();
        byte[] headerHash = hash(hb.block.getHeader(), params.digestAlgorithm()).getBytes();
        return hb.certifiedBlock.getCertificationsList().stream().filter(c -> validate(headerHash, c, validators))
                                .count() > params.context().toleranceLevel() + 1;
    }

    default boolean validateRegeneration(HashedCertifiedBlock hb) {
        if (!hb.block.hasGenesis()) {
            return false;
        }
        var reconfigure = hb.block.getGenesis().getInitialView();
        var validators = validatorsOf(reconfigure, params().context());
        Sets.difference(validators.keySet(),
                        new HashSet<>(params().context().successors(new Digest(reconfigure.getId()))))
            .forEach(m -> validators.remove(m));
        var headerHash = HashedBlock.hash(hb.block.getHeader(), params().digestAlgorithm()).getBytes();
        return hb.certifiedBlock.getCertificationsList().stream().filter(c -> validate(headerHash, c, validators))
                                .count() > params().context().toleranceLevel() + 1;
    }
}
