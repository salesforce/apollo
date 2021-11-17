/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static com.salesforce.apollo.membership.Member.getMemberIdentifier;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.List;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.comm.grpc.ClientContextSupplier;
import com.salesforce.apollo.comm.grpc.ServerContextSupplier;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * @author hal.hildebrand
 *
 */
public interface SigningMember extends Member, Signer, ServerContextSupplier, ClientContextSupplier {

    @Override
    default Digest getMemberId(X509Certificate key) {
        return getMemberIdentifier(key);
    }

    default JohnHancock sign(byte[]... message) {
        return sign(BbBackedInputStream.aggregate(message));
    }

    default JohnHancock sign(ByteBuffer... message) {
        return sign(BbBackedInputStream.aggregate(message));
    }

    default JohnHancock sign(ByteString... message) {
        return sign(BbBackedInputStream.aggregate(message));
    }

    JohnHancock sign(InputStream message);

    default JohnHancock sign(List<ByteBuffer> buffers) {
        return sign(BbBackedInputStream.aggregate(buffers));
    }

}
