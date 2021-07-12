/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;

/**
 * @author hal.hildebrand
 *
 */
public record Config(short nProc, int epochLength, short pid, int zeroVotRoundForCommonVote, int firstDecidedRound,
                     int orderStartLevel, int commonVoteDeterministicPrefix, short crpFixedPrefix, Signer signer,
                     DigestAlgorithm digestAlgorithm, int lastLevel, boolean canSkipLevel, int numberOfEpochs) {

}
