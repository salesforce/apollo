/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState;
import com.salesforce.apollo.stereotomy.services.ProtoResolverService;

/**
 * @author hal.hildebrand
 *
 */
@Path("/resolver")
@Consumes("application/x-protobuf")
@Produces("application/x-protobuf")
public class ResolverResource {

    private final ProtoResolverService resolver;

    public ResolverResource(ProtoResolverService resolver) {
        this.resolver = resolver;
    }

    @PUT
    @Path("bind")
    public void bind(Binding binding) {
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
    }
}
