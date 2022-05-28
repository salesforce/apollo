/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.security.KeyPair;
import java.util.Optional;

import org.h2.jdbcx.JdbcConnectionPool;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;

/**
 * The witness and validation service in Thoth
 * 
 * @author hal.hildebrand
 *
 */
public class Sakshi {

    @SuppressWarnings("unused")
    private final JdbcConnectionPool         jdbcPool;
    @SuppressWarnings("unused")
    private final Stereotomy                 stereotomy;
    private volatile ControlledIdentifier<?> validator;
    private volatile KeyPair                 witness;

    public Sakshi(KERL kerl, Stereotomy stereotomy, JdbcConnectionPool jdbcPool) {
        this.stereotomy = stereotomy;
        this.jdbcPool = jdbcPool;
    }

    public BasicIdentifier getWitness() {
        KeyPair current = witness;
        return new BasicIdentifier(current.getPublic());
    }

    public JohnHancock validate(KeyEvent event) {
        Optional<Signer> signer = validator.getSigner();
        if (signer.isEmpty()) {
            return null;
        }
        return signer.get().sign(event.toKeyEvent_().toByteString());
    }

    public JohnHancock witness(KeyEvent event, BasicIdentifier identifier) {
        if (!witness.getPublic().equals(identifier.getPublicKey())) {
            return null;
        }
        return new Signer.SignerImpl(witness.getPrivate()).sign(event.toKeyEvent_().toByteString());
    }
}
