/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.utils.BbBackedInputStream;

public interface ControllableIdentifier extends KeyState {
    ControllableIdentifier bind();

    Signer getSigner(int keyIndex);

    Verifier getVerifier();

    void rotate();

    void rotate(List<Seal> seals);

    void rotate(RotationSpecification.Builder spec);

    void seal(InteractionSpecification.Builder spec);

    void seal(List<Seal> seals);

    default JohnHancock sign(byte[]... buffs) {
        return sign(BbBackedInputStream.aggregate(buffs));
    }

    default JohnHancock sign(ByteBuffer... buffs) {
        return sign(BbBackedInputStream.aggregate(buffs));
    }

    default JohnHancock sign(ByteString... buffs) {
        return sign(BbBackedInputStream.aggregate(buffs));
    }

    JohnHancock sign(InputStream is);

    EventSignature sign(KeyEvent event);

}
