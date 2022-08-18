/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

/**
 * The DAO
 *
 * @author hal.hildebrand
 *
 */
public interface Vault {

    void add(String name, byte[] password, boolean admin);

    void changePassword(String name, byte[] password, byte[] newPassword);

    boolean contains(String name);

}
