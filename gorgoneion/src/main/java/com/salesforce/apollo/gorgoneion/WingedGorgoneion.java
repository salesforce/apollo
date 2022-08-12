/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import org.bouncycastle.crypto.generators.SCrypt;

import com.google.protobuf.Any;
import com.salesfoce.apollo.gorgoneion.proto.ChangePassword;
import com.salesfoce.apollo.gorgoneion.proto.CreateUser;
import com.salesfoce.apollo.gorgoneion.proto.DecryptRequest;
import com.salesfoce.apollo.gorgoneion.proto.DecryptResponse;
import com.salesfoce.apollo.gorgoneion.proto.Delegation;
import com.salesfoce.apollo.gorgoneion.proto.EncryptionRequest;
import com.salesfoce.apollo.gorgoneion.proto.EncryptionResponse;
import com.salesfoce.apollo.gorgoneion.proto.Export;
import com.salesfoce.apollo.gorgoneion.proto.Modify;
import com.salesfoce.apollo.gorgoneion.proto.Owners;
import com.salesfoce.apollo.gorgoneion.proto.Summary;
import com.salesfoce.apollo.gorgoneion.proto.UsernamePassword;

/**
 * @author hal.hildebrand
 *
 */
public class WingedGorgoneion {
    private final SCrypt scrypt;

    public WingedGorgoneion(SCrypt scrypt) {
        this.scrypt = scrypt;
    }

    public void changePassword(ChangePassword request) {

    }

    public void create(UsernamePassword up) {

    }

    public void createUser(CreateUser user) {

    }

    public DecryptResponse decrypt(DecryptRequest request) {
        return null;
    }

    public void delegate(Delegation delegation) {

    }

    public EncryptionResponse encrypt(EncryptionRequest request) {
        return null;
    }

    public Export export(UsernamePassword up) {
        return null;
    }

    public void modify(Modify request) {

    }

    public Owners owners(Any data) {
        return null;
    }

    public void purge(UsernamePassword up) {

    }

    public EncryptionResponse reencrypt(EncryptionRequest request) {
        return null;
    }

    public Summary summary() {
        return null;
    }
}
