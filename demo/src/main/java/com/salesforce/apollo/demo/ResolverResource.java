/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState;
import com.salesforce.apollo.stereotomy.services.ProtoResolverService;
import com.salesforce.apollo.stereotomy.services.ProtoResolverService.BinderService;

/**
 * @author hal.hildebrand
 *
 */
@Path("/resolver")
@Consumes("application/x-protobuf")
@Produces("application/x-protobuf")
public class ResolverResource {

    private final BinderService        binder;
    private final ProtoResolverService resolver;
    private final Duration             timeout;

    public ResolverResource(ProtoResolverService resolver, BinderService binder, Duration timeout) {
        this.resolver = resolver;
        this.binder = binder;
        this.timeout = timeout;
    }

    @PUT
    @Path("append")
    public void append(KeyEvent event) {
        try {
            binder.append(event).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @PUT
    @Path("bind")
    public void bind(Binding binding) {
        try {
            binder.bind(binding).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @POST
    @Path("kerl")
    public KERL kerl(Ident identifier) {
        return resolver.kerl(identifier).get();
    }

    @POST
    @Path("lookup")
    public Binding lookup(Ident identifier) {
        return resolver.lookup(identifier).get();
    }

    @PUT
    @Path("publish")
    public void publish(KERL kerl) {
        try {
            binder.publish(kerl).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @POST
    @Path("resolve/coords")
    public KeyState resolve(EventCoords coordinates) {
        return resolver.resolve(coordinates).get();
    }

    @POST
    @Path("resolve")
    public KeyState resolve(Ident identifier) {
        return resolver.resolve(identifier).get();
    }

    @DELETE
    @Path("unbind")
    public void unbind(Ident identifier) {
        try {
            binder.unbind(identifier);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }
}
