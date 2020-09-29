/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;

import io.grpc.BindableService;

/**
 * @author hal.hildebrand
 *
 */
public interface ClientFactory {

    <T> CommonCommunications<T> create(Member member, CreateClientCommunications<T> createFunction, BindableService service);

}
