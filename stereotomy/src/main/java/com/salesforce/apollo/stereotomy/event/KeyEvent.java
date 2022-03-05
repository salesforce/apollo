/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import org.joou.ULong;

import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments.Builder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface KeyEvent {

    public static final String DELEGATED_INCEPTION_TYPE       = "dip";
    public static final String DELEGATED_ROTATION_TYPE        = "drt";
    public static final String INCEPTION_TYPE                 = "icp";
    public static final String INTERACTION_TYPE               = "ixn";
    public static final String NONE                           = "nan";
    public static final String RECEIPT_FROM_BASIC_TYPE        = "rct";
    public static final String RECEIPT_FROM_TRANSFERABLE_TYPE = "vrc";
    public static final String ROTATION_TYPE                  = "rot";

    JohnHancock getAuthentication();

    byte[] getBytes();

    default EventCoordinates getCoordinates() {
        return new EventCoordinates(getIdentifier(), getSequenceNumber(), getPriorEventDigest(), getIlk());
    }

    Format getFormat();

    Identifier getIdentifier();

    String getIlk();

    EventCoordinates getPrevious();

    Digest getPriorEventDigest();

    ULong getSequenceNumber();

    Version getVersion();

    Digest hash(DigestAlgorithm digest);

    void setEventOf(Builder builder);
}
