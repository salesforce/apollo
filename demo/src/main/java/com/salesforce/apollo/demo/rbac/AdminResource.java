/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo.rbac;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Timed;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.delphinius.Oracle.Assertion;
import com.salesforce.apollo.delphinius.Oracle.Namespace;
import com.salesforce.apollo.delphinius.Oracle.Object;
import com.salesforce.apollo.delphinius.Oracle.Relation;
import com.salesforce.apollo.delphinius.Oracle.Subject;

/**
 * @author hal.hildebrand
 *
 */
@Path("/rbac/admin")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AdminResource {

    record PredicateObject(Relation predicate, Object object) {}

    record Assocation<T> (T a, T b) {}

    record PredicateObjects(Relation predicate, List<Object> objects) {}

    record PredicateSubject(Relation predicate, Subject subject) {}

    @SuppressWarnings("unused")
    private final Oracle oracle;

    public AdminResource(Oracle oracle) {
        this.oracle = oracle;
    }

    @PUT
    @Timed
    @Path("add/assertion")
    public void add(Assertion assertion) {
    }

    @PUT
    @Timed
    @Path("add/namespace")
    public void add(Namespace namespace) {
    }

    @PUT
    @Timed
    @Path("add/object")
    public void add(Object object) {
    }

    @PUT
    @Timed
    @Path("add/relation")
    public void add(Relation relation) {
    }

    @PUT
    @Timed
    @Path("add/subject")
    public void add(Subject subject) {
    }

    @POST
    @Timed
    @Path("delete/assertion")
    public void delete(Assertion assertion) {
    }

    @POST
    @Timed
    @Path("delete/namespace")
    public void delete(Namespace namespace) {
    }

    @POST
    @Timed
    @Path("delete/object")
    public void delete(Object object) {
    }

    @POST
    @Timed
    @Path("delete/relation")
    public void delete(Relation relation) {
    }

    @POST
    @Timed
    @Path("delete/subject")
    public void delete(Subject subject) {
    }

    @POST
    @Timed
    @Path("expand/object")
    public Response expand(Object object) {
        return null;
    }

    @POST
    @Timed
    @Path("expand/objects")
    public Response expand(PredicateObject predicateObject) {
        return null;
    }

    @POST
    @Timed
    @Path("expand/subjects")
    public Response expand(PredicateSubject predicateSubject) {
        return null;
    }

    @POST
    @Timed
    @Path("expand/subject")
    public Response expand(Subject subject) {
        return null;
    }

    @PUT
    @Timed
    @Path("map/object")
    public void mapObject(Assocation<Object> association) {
    }

    @PUT
    @Timed
    @Path("map/relation")
    public void mapRelation(Assocation<Relation> association) {
    }

    @PUT
    @Timed
    @Path("map/subject")
    public void mapSubject(Assocation<Subject> association) {
    }

    @POST
    @Timed
    @Path("read/objects/subjects")
    public Response read(PredicateObjects predicateObjects) {
        return null;
    }

    @POST
    @Timed
    @Path("read/subjects/objects")
    public Response read(PredicateSubject predicateSubject) {
        return null;
    }

    @POST
    @Timed
    @Path("read/subjects")
    public Response readObjects(List<Object> objects) {
        return null;
    }

    @POST
    @Timed
    @Path("read/objects")
    public Response readSubjects(List<Subject> subjects) {
        return null;
    }

    @POST
    @Timed
    @Path("remove/object")
    public void removeObjectMapping(Assocation<Object> association) {
    }

    @POST
    @Timed
    @Path("remove/relation")
    public void removeRelationMapping(Assocation<Relation> association) {
    }

    @POST
    @Timed
    @Path("remove/subject")
    public void removeSubjectMapping(Assocation<Subject> association) {
    }

    @POST
    @Timed
    @Path("subjects")
    public Response subjects(PredicateObject predicateObject) {
        return null;
    }
}
