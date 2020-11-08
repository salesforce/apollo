/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.security.PublicKey;
import java.security.Signature;

import com.salesforce.apollo.membership.Member;

public class Collaborator extends Member {
    public final PublicKey consensusKey;

    public Collaborator(Member member, byte[] consensusKey) {
        this(member, Validator.publicKeyOf(consensusKey));
    }

    public Collaborator(Member member, PublicKey consensusKey) {
        super(member.getId(), member.getCertificate());
        this.consensusKey = consensusKey;
    }

    /**
     * Answer the Signature, initialized with the member's public consensus key,
     * using the supplied signature algorithm.
     * 
     * @param signatureAlgorithm
     * @return the signature, initialized for verification
     */
    public Signature forValidation(String signatureAlgorithm) {
        PublicKey key = consensusKey;
        return Validator.signatureForVerification(key);
    }
}