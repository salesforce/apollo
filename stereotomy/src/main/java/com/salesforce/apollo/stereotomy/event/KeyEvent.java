/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.proto.KeyEventWithAttachments.Builder;
import com.salesforce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.joou.ULong;

/**
 * @author hal.hildebrand
 */
public interface KeyEvent {

    String DELEGATED_INCEPTION_TYPE = "dip";
    String DELEGATED_ROTATION_TYPE  = "drt";
    String INCEPTION_TYPE           = "icp";
    String INTERACTION_TYPE         = "ixn";
    String NONE                     = "nan";
    String ROTATION_TYPE            = "rot";

    JohnHancock getAuthentication();

    byte[] getBytes();

    default EventCoordinates getCoordinates() {
        return new EventCoordinates(getIdentifier(), getSequenceNumber(), getPriorEventDigest(), getIlk());
    }

    Identifier getIdentifier();

    String getIlk();

    EventCoordinates getPrevious();

    Digest getPriorEventDigest();

    ULong getSequenceNumber();

    Version getVersion();

    Digest hash(DigestAlgorithm digest);

    void setEventOf(Builder builder);

    KeyEvent_ toKeyEvent_();
}
