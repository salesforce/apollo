/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */

package com.salesforce.apollo.web.resources;

import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.salesforce.apollo.avalanche.DagDao;
import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;

/**
 * @author hhildebrand
 */
@Path("/api/dag")
public class DagApi {

    public static class DagNode {
        private String data;
        private String description;
        private List<String> links;

        public DagNode() {}

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

        public QueryFinalizedResult() {}

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
    @Path("fetch")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    @Timed
    public String fetch(String key) {
        HASH hash = new HASH(DECODER.decode(key));
        DagEntry dagEntry = dag.get(hash);
        if (dagEntry == null) { return null; }
        return dagEntry.getData() == null ? null
                : ENCODER.encodeToString(dagEntry.getData().array());
    }

    @POST()
    @Path("fetchDagNode")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public DagNode fetchDagNode(String key) {
        HASH hash = new HASH(DECODER.decode(key));
        DagEntry dagEntry = dag.get(hash);
        if (dagEntry == null) { return null; }
        List<String> links = dagEntry.getLinks() == null ? null
                : dagEntry.getLinks().stream().map(h -> ENCODER.encodeToString(h.bytes())).collect(Collectors.toList());
        return new DagNode(dagEntry.getData() == null ? null
                : ENCODER.encodeToString(dagEntry.getData().array()), links,
                           ENCODER.encodeToString(dagEntry.getData().array()));
    }

    @POST()
    @Path("queryFinalized")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    public QueryFinalizedResult queryFinalized(String key) {
        HASH hash = new HASH(DECODER.decode(key));
        Boolean finalized = dag.isFinalized(hash);
        return new QueryFinalizedResult(finalized == null ? false : finalized);
    }

}
