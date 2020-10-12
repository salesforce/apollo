/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

/**
 * @author hal.hildebrand
 *
 */
public interface ClientIdentity {

    X509Certificate getCert();

    Certificate[] getCerts();

    HashKey getFrom();

}
