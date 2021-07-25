/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.choam.support.HashedBlock.hash;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.Reconfigure;
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

    public abstract class CommitteeCommon implements Committee {
        protected final Map<Member, Verifier> validators;
        private final long                    height;
        private final Digest                  id;

        public CommitteeCommon(HashedBlock block, Context<Member> context) {
            this(block.height(), new Digest(block.block.getReconfigure().getId()),
                 validatorsOf(block.block.getReconfigure(), context));
        }

        public CommitteeCommon(long height, Digest id, Map<Member, Verifier> validators) {
            this.height = height;
            this.id = id;
            this.validators = validators;
        }

        @Override
        public long getHeight() {
            return height;
        }

        @Override
        public Digest getId() {
            return id;
        }

        @Override
        public boolean validate(HashedCertifiedBlock hb) {
            Parameters params = params();
            byte[] headerHash = hash(hb.block.getHeader(), params.digestAlgorithm()).getBytes();
            return hb.certifiedBlock.getCertificationsList().stream().filter(c -> validate(headerHash, c, validators))
                                    .count() > params.context().toleranceLevel() + 1;
        }
    }

    static final Logger log = LoggerFactory.getLogger(CommitteeCommon.class);

    static Map<Member, Verifier> validatorsOf(Reconfigure reconfigure, Context<Member> context) {
        return reconfigure.getViewList().stream()
                          .collect(Collectors.toMap(e -> context.getMember(new Digest(e.getId())),
                                                    e -> new DefaultVerifier(publicKey(e.getConsensusKey()))));
    }

    void accept(HashedCertifiedBlock next);

    long getHeight();

    Digest getId();

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

    boolean validate(HashedCertifiedBlock block);
}
