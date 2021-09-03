/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("rawtypes")
public record SubmittedTransaction(Digest hash, Transaction transaction, CompletableFuture onCompletion) {}
