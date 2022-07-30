/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.thoth.proto.KeyStateWithEndorsementsAndValidations;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

/**
 * @author hal.hildebrand
 *
 */
public interface Dht extends ProtoKERLService {
    @Override
    CompletableFuture<Empty> appendValidations(Validations validations);

    CompletableFuture<KeyStateWithEndorsementsAndValidations> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates);

    @Override
    CompletableFuture<Validations> getValidations(EventCoords coordinates);
}
