/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.web.resources;

import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.codahale.metrics.annotation.Timed;
import com.salesforce.apollo.avalanche.Processor.TimedProcessor;
import com.salesforce.apollo.protocols.HashKey;

@Path("/api/genesisBlock")
public class GenesisBlockApi {

    public static class Result {
        public boolean error;
        public String  errorMessage;
        public String  hash;

        public Result() {
        }

        public Result(HashKey hash, boolean error, String errorMessage) {
            this.hash = hash == null ? null : hash.b64Encoded();
            this.error = error;
            this.errorMessage = errorMessage;
        }
    }

    private final TimedProcessor           processor;
    private final ScheduledExecutorService scheduler;

    public GenesisBlockApi(TimedProcessor avalanche, ScheduledExecutorService scheduler) {
        this.processor = avalanche;
        this.scheduler = scheduler;
    }

    @POST()
    @Path("create")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public Result create(String encoded) {
        if (encoded == null) {
            throw new WebApplicationException(
                    Response.status(Status.BAD_REQUEST).entity("Encoded transaction content cannot be null").build());
        }

        byte[] data;
        try {
            data = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException(
                    Response.status(Status.BAD_REQUEST).entity("Cannot decode B64 url encoded content").build());
        }

        CompletableFuture<HashKey> submitted = processor.createGenesis(data, Duration.ofMillis(30_000), scheduler);
        HashKey result;
        try {
            result = submitted.get(30_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return new Result(null, true, "Interrupted");
        } catch (ExecutionException e) {
            return new Result(null, true, e.getCause().getMessage());
        } catch (TimeoutException e) {
            return new Result(null, true, "Timed out");
        }
        return result == null ? new Result(null, true, "Timed out") : new Result(null, false, encoded);
    }

}
