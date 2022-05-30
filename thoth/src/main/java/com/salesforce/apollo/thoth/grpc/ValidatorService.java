/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc;

import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.thoth.proto.Signatures;
import com.salesfoce.apollo.thoth.proto.Validated;
import com.salesforce.apollo.comm.Link;

/**
 * @author hal.hildebrand
 *
 */
public interface ValidatorService extends Link {

    ListenableFuture<Validated> validate(List<KeyEventWithAttachments> events);

    ListenableFuture<Signatures> witness(List<KeyEvent_> events, Ident identifier);

}
