/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.salesforce.apollo.archipelago.Router.ClientConnector;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * 
 *         A routable communications overlay
 */
public interface Communications<Client extends Link, Service> extends ClientConnector<Client> {

    @Override
    Client connect(Member to);

    void deregister(Digest context);

    void register(Digest context, Service service);

}
