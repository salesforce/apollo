/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

/**
 * @author hal.hildebrand
 *
 */
@Path("/kerl")
@Consumes("application/x-protobuf")
@Produces("application/x-protobuf")
public class KERLResource {

    private final ProtoKERLService resolver;
    private final Duration         timeout;

    public KERLResource(ProtoKERLService resolver, Duration timeout) {
        this.resolver = resolver;
        this.timeout = timeout;
    }

    @PUT
    @Path("append/eventvent")
    public void append(KeyEvent_ event) {
        try { // TODO fix args
            resolver.append(Collections.singletonList(event)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @POST
    @Path("get/KERL")
    public KERL_ getKERL(Ident identifier) {
        return resolver.getKERL(identifier).get();
    }

    @POST
    @Path("get/key/state/coords")
    public KeyState_ resolve(EventCoords coordinates) {
        return resolver.getKeyState(coordinates).get();
    }

    @POST
    @Path("get/key/state")
    public KeyState_ resolve(Ident identifier) {
        return resolver.getKeyState(identifier).get();
    }
}
