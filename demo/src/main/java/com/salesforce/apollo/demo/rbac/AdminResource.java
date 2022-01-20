/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo.rbac;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
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

    public record PredicateObject(Relation predicate, Object object) {}

    public record Assocation<T> (T a, T b) {}

    public record PredicateObjects(Relation predicate, List<Object> objects) {}

    public record PredicateSubject(Relation predicate, Subject subject) {}

    private final Oracle oracle;

    public AdminResource(Oracle oracle) {
        this.oracle = oracle;
    }

    @PUT
    @Timed
    @Path("add/assertion")
    public void add(Assertion assertion) {
        try {
            oracle.add(assertion).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @PUT
    @Timed
    @Path("add/namespace")
    public void add(Namespace namespace) {
        try {
            oracle.add(namespace).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @PUT
    @Timed
    @Path("add/object")
    public void add(Object object) {
        try {
            oracle.add(object).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @PUT
    @Timed
    @Path("add/relation")
    public void add(Relation relation) {
        try {
            oracle.add(relation).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @PUT
    @Timed
    @Path("add/subject")
    public void add(Subject subject) {
        try {
            oracle.add(subject).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("delete/assertion")
    public void delete(Assertion assertion) {
        try {
            oracle.delete(assertion).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("delete/namespace")
    public void delete(Namespace namespace) {
        try {
            oracle.delete(namespace).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("delete/object")
    public void delete(Object object) {
        try {
            oracle.delete(object).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("delete/relation")
    public void delete(Relation relation) {
        try {
            oracle.delete(relation).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("delete/subject")
    public void delete(Subject subject) {
        try {
            oracle.delete(subject).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("expand/object")
    public List<Subject> expand(Object object) {
        try {
            return oracle.expand(object);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("expand/objects")
    public List<Subject> expand(PredicateObject predicateObject) {
        try {
            return oracle.expand(predicateObject.predicate, predicateObject.object);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("expand/subjects")
    public List<Object> expand(PredicateSubject predicateSubject) {
        try {
            return oracle.expand(predicateSubject.predicate, predicateSubject.subject);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("expand/subject")
    public List<Object> expand(Subject subject) {
        try {
            return oracle.expand(subject);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @PUT
    @Timed
    @Path("map/object")
    public void mapObject(Assocation<Object> association) {
        try {
            oracle.map(association.a, association.b).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @PUT
    @Timed
    @Path("map/relation")
    public void mapRelation(Assocation<Relation> association) {
        try {
            oracle.map(association.a, association.b).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @PUT
    @Timed
    @Path("map/subject")
    public void mapSubject(Assocation<Subject> association) {
        try {
            oracle.map(association.a, association.b).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("read/objects/subjects")
    public List<Subject> read(PredicateObjects predicateObjects) {
        try {
            return oracle.read(predicateObjects.predicate,
                               predicateObjects.objects.toArray(new Object[predicateObjects.objects.size()]));
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
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
    public List<Subject> readObjects(List<Object> objects) {
        try {
            return oracle.read(objects.toArray(new Object[objects.size()]));
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("read/objects")
    public List<Object> readSubjects(List<Subject> subjects) {
        try {
            return oracle.read(subjects.toArray(new Subject[subjects.size()]));
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("remove/object")
    public void removeObjectMapping(Assocation<Object> association) {
        try {
            oracle.remove(association.a, association.b).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("remove/relation")
    public void removeRelationMapping(Assocation<Relation> association) {
        try {
            oracle.remove(association.a, association.b).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("remove/subject")
    public void removeSubjectMapping(Assocation<Subject> association) {
        try {
            oracle.remove(association.a, association.b).get();
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Timed
    @Path("subjects")
    public Stream<Subject> subjects(PredicateObject predicateObject) {
        try {
            return oracle.subjects(predicateObject.predicate, predicateObject.object);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
