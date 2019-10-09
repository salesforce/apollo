/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.web.resources;

import java.time.Duration;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.codahale.metrics.annotation.Timed;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.WellKnownDescriptions;
import com.salesforce.apollo.protocols.HashKey;

@Path("/api/byteTransaction")
public class ByteTransactionApi {
    public static class ByteTransaction {
        private static final Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

        public String encoded;
        public int timeoutMillis;

        public ByteTransaction() {}

        public ByteTransaction(int timeoutMillis, byte[] content) {
            this(timeoutMillis, new String(ENCODER.encode(content)));
        }

        public ByteTransaction(int timeoutMillis, String encoded) {
            super();
            this.timeoutMillis = timeoutMillis;
            this.encoded = encoded;
        }
    }

    public static class TransactionResult {
        public boolean error;
        public String errorMessage;
        public String result;

        public TransactionResult() {}

        public TransactionResult(HashKey result) {
            this(new String(Base64.getUrlEncoder().withoutPadding().encode(result.bytes())));
        }

        public TransactionResult(String result) {
            this(result, false, null);
        }

        public TransactionResult(String result, boolean error, String errorMessage) {
            this.result = result;
            this.error = error;
            this.errorMessage = errorMessage;
        }
    }

    private final Avalanche avalanche;
    private final ScheduledExecutorService scheduler;

    public ByteTransactionApi(Avalanche avalanche, ScheduledExecutorService scheduler) {
        this.avalanche = avalanche;
        this.scheduler = scheduler;
    }

    @POST()
    @Path("submit")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TransactionResult submit(ByteTransaction transaction) {
        if (transaction.encoded == null) {
            throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
                                                      .entity("Encoded transaction content cannot be null")
                                                      .build());
        }

        byte[] data;
        try {
            data = Base64.getDecoder().decode(transaction.encoded);
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
                                                      .entity("Cannot decode B64 url encoded content")
                                                      .build());
        }

        CompletableFuture<HashKey> submitted = avalanche.submitTransaction(WellKnownDescriptions.BYTE_CONTENT.toHash(),
                                                                           data,
                                                                           Duration.ofMillis(transaction.timeoutMillis),
                                                                           scheduler);
        HashKey result;
        try {
            result = submitted.get(transaction.timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return new TransactionResult(null, true, "Interrupted");
        } catch (ExecutionException e) {
            return new TransactionResult(null, true, e.getCause().getMessage());
        } catch (TimeoutException e) {
            return new TransactionResult(null, true, "Timed out");
        }
        return result == null ? new TransactionResult(null, true, "Timed out") : new TransactionResult(result);
    }

    @POST()
    @Path("submitAsync")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public String submitAsync(ByteTransaction transaction) {
        if (transaction.encoded == null) {
            throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
                                                      .entity("Encoded transaction content cannot be null")
                                                      .build());
        }

        byte[] data;
        try {
            data = Base64.getDecoder().decode(transaction.encoded);
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
                                                      .entity("Cannot decode B64 url encoded content")
                                                      .build());
        }

        return Base64.getUrlEncoder()
                     .withoutPadding()
                     .encodeToString(avalanche.submitTransaction(WellKnownDescriptions.BYTE_CONTENT.toHash(),
                                                                      data,
                                                                      Duration.ofMillis(transaction.timeoutMillis),
                                                                      null,
                                                                      scheduler)
                                              .bytes());
    }

}
