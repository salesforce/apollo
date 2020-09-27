/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.web.resources;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.codahale.metrics.annotation.Timed;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.avalanche.DagDao;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hhildebrand
 */
@Path("/api/dag")
public class DagApi {

    public static class DagNode {
        private String       data;
        private String       description;
        private List<String> links;

        public DagNode() {
        }

        public DagNode(String data, List<String> links, String description) {
            this.data = data;
            this.links = links;
            this.description = description;
        }

        public String getData() {
            return data;
        }

        public String getDescription() {
            return description;
        }

        public List<String> getLinks() {
            return links;
        }
    }

    public static class QueryFinalizedResult {
        private boolean finalized;

        public QueryFinalizedResult() {
        }

        public QueryFinalizedResult(boolean finalized) {
            this.finalized = finalized;
        }

        public boolean isFinalized() {
            return finalized;
        }
    }

    private static final Decoder DECODER = Base64.getUrlDecoder();
    private static final Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final DagDao dag;

    public DagApi(DagDao dagDao) {
        dag = dagDao;
    }

    @POST()
    @Path("allFinalized")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public String[] allFinalized() {
        final List<String> result = dag.allFinalized()
                                       .stream()
                                       .map(key -> ENCODER.encodeToString(key.bytes()))
                                       .collect(Collectors.toList());
        return result.toArray(new String[result.size()]);
    }

    @POST()
    @Path("fetch")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    @Timed
    public String fetch(String key) {
        HashKey hash = new HashKey(DECODER.decode(key));
        DagEntry dagEntry = dag.get(hash);
        if (dagEntry == null) {
            return null;
        }
        return dagEntry.getData() == null ? null : ENCODER.encodeToString(dagEntry.getData().toByteArray());
    }

    @POST()
    @Path("fetchDagNode")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public DagNode fetchDagNode(String key) {
        HashKey hash = new HashKey(DECODER.decode(key));
        DagEntry dagEntry = dag.get(hash);
        if (dagEntry == null) {
            return null;
        }
        List<String> links = dagEntry.getLinksCount() == 0 ? null
                : dagEntry.getLinksList()
                          .stream()
                          .map(h -> ENCODER.encodeToString(h.toByteArray()))
                          .collect(Collectors.toList());
        return new DagNode(dagEntry.getData() == null ? null : ENCODER.encodeToString(dagEntry.getData().toByteArray()),
                links, ENCODER.encodeToString(dagEntry.getData().toByteArray()));
    }

    @POST()
    @Path("queryAllFinalized")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Boolean[] queryAllFinalized(String[] query) {
        if (query == null) {
            throw new WebApplicationException(
                    Response.status(Status.BAD_REQUEST).entity("Encoded transaction content cannot be null").build());
        }

        List<Boolean> result = new ArrayList<>();
        for (String t : query) {
            result.add(dag.isFinalized(new HashKey(DECODER.decode(t))));
        }
        return result.toArray(new Boolean[result.size()]);
    }

    @POST()
    @Path("queryFinalized")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public QueryFinalizedResult queryFinalized(String key) {
        HashKey hash = new HashKey(DECODER.decode(key));
        Boolean finalized = dag.isFinalized(hash);
        return new QueryFinalizedResult(finalized == null ? false : finalized);
    }

}
