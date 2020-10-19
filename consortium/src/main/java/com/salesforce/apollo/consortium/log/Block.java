/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.log;

import com.salesforce.apollo.consortium.Certification;

/**
 * @author hal.hildebrand
 *
 */
public class Block {
    private final Body          body;
    private final Certification cert;
    private final Header        header;

    public Block(Header header, Body body, Certification cert) {
        this.header = header;
        this.body = body;
        this.cert = cert;
    }

    public Body getBody() {
        return body;
    }

    public Certification getCert() {
        return cert;
    }

    public Header getHeader() {
        return header;
    }
}
