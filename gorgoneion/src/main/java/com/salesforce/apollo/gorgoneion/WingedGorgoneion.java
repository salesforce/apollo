/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import java.nio.charset.StandardCharsets;

import com.google.protobuf.Any;
import com.salesfoce.apollo.gorgoneion.proto.ChangePassword;
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

import io.grpc.Status;
import io.grpc.StatusException;

/**
 * @author hal.hildebrand
 *
 */
public class WingedGorgoneion {
    private final Vault vault;

    public WingedGorgoneion(Vault vault) {
        this.vault = vault;
    }

    public void changePassword(ChangePassword request) throws StatusException {
        if (!validate(request.getCredentials())) {
            throw new StatusException(Status.INVALID_ARGUMENT.withDescription("Username or Password invalid"));
        }
        if (!vault.contains(request.getCredentials().getName())) {
            throw new StatusException(Status.NOT_FOUND.withDescription("Username: " + request.getCredentials().getName()
            + " does not exist"));
        }
        vault.changePassword(request.getCredentials().getName(),
                             request.getCredentials().getPassword().getBytes(StandardCharsets.UTF_8),
                             request.getNewPassword().getBytes(StandardCharsets.UTF_8));
    }

    public void create(UsernamePassword up) throws StatusException {
        if (!validate(up)) {
            throw new StatusException(Status.INVALID_ARGUMENT.withDescription("Username or Password invalid"));
        }
    }

    public void createUser(UsernamePassword up) throws StatusException {
        if (!validate(up)) {
            throw new StatusException(Status.INVALID_ARGUMENT.withDescription("Username or Password invalid"));
        }
        if (vault.contains(up.getName())) {
            throw new StatusException(Status.ALREADY_EXISTS.withDescription("Username: " + up.getName()
            + " already exists"));
        }
        vault.add(up.getName(), up.getPassword().getBytes(StandardCharsets.UTF_8), false);
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

    private boolean validate(UsernamePassword up) {
        if (up.getName().isBlank() || up.getName().isEmpty() || up.getPassword().isBlank() ||
            up.getPassword().isEmpty()) {
            return false;
        }
        return true;
    }
}
