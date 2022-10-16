/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https:/opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import com.codahale.metrics.annotation.Timed;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.delphinius.Oracle.Assertion;
import com.salesforce.apollo.delphinius.Oracle.Namespace;
import com.salesforce.apollo.delphinius.Oracle.Object;
import com.salesforce.apollo.delphinius.Oracle.Relation;
import com.salesforce.apollo.delphinius.Oracle.Subject;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * @author hal.hildebrand
 *
 */
@Path("/delphi")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DelphiResource {

    public record PredicateObject(Relation predicate, Object object) {}

    public record Assocation<T> (T a, T b) {}

    public record PredicateObjects(Relation predicate, List<Object> objects) {}

    public record PredicateSubject(Relation predicate, Subject subject) {}

    private final Oracle   oracle;
    private final Duration timeout;

    public DelphiResource(Oracle oracle, Duration timeout) {
        this.oracle = oracle;
        this.timeout = timeout;
    }

    @PUT
    @Timed
    @Path("admin/add/assertion")
    public void add(Assertion assertion) {
        try {
            oracle.add(assertion).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @PUT
    @Timed
    @Path("admin/add/namespace")
    public void add(Namespace namespace) {
        try {
            oracle.add(namespace).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @PUT
    @Timed
    @Path("admin/add/object")
    public void add(Object object) {
        try {
            oracle.add(object).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @PUT
    @Timed
    @Path("admin/add/relation")
    public void add(Relation relation) {
        try {
            oracle.add(relation).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @PUT
    @Timed
    @Path("admin/add/subject")
    public void add(Subject subject) {
        try {
            oracle.add(subject).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @POST
    @Timed
    @Path("check")
    public boolean check(Assertion assertion) {
        try {
            return oracle.check(assertion);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @DELETE
    @Timed
    @Path("admin/delete/assertion")
    public void delete(Assertion assertion) {
        try {
            oracle.delete(assertion).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @DELETE
    @Timed
    @Path("admin/delete/namespace")
    public void delete(Namespace namespace) {
        try {
            oracle.delete(namespace).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @DELETE
    @Timed
    @Path("admin/delete/object")
    public void delete(Object object) {
        try {
            oracle.delete(object).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @DELETE
    @Timed
    @Path("admin/delete/relation")
    public void delete(Relation relation) {
        try {
            oracle.delete(relation).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @DELETE
    @Timed
    @Path("admin/delete/subject")
    public void delete(Subject subject) {
        try {
            oracle.delete(subject).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @POST
    @Timed
    @Path("admin/expand/object")
    public List<Subject> expand(Object object) {
        try {
            return oracle.expand(object);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("admin/expand/objects")
    public List<Subject> expand(PredicateObject predicateObject) {
        try {
            return oracle.expand(predicateObject.predicate, predicateObject.object);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("admin/expand/subjects")
    public List<Object> expand(PredicateSubject predicateSubject) {
        try {
            return oracle.expand(predicateSubject.predicate, predicateSubject.subject);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("admin/expand/subject")
    public List<Object> expand(Subject subject) {
        try {
            return oracle.expand(subject);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @PUT
    @Timed
    @Path("admin/map/object")
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
    @Path("admin/map/relation")
    public void mapRelation(Assocation<Relation> association) {
        try {
            oracle.map(association.a, association.b).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @PUT
    @Timed
    @Path("admin/map/subject")
    public void mapSubject(Assocation<Subject> association) {
        try {
            oracle.map(association.a, association.b).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @POST
    @Timed
    @Path("admin/read/objects/subjects")
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
    @Path("admin/read/subjects/objects")
    public Response read(PredicateSubject predicateSubject) {
        return null;
    }

    @POST
    @Timed
    @Path("admin/read/subjects")
    public List<Subject> readObjects(List<Object> objects) {
        try {
            return oracle.read(objects.toArray(new Object[objects.size()]));
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Timed
    @Path("admin/read/objects")
    public List<Object> readSubjects(List<Subject> subjects) {
        try {
            return oracle.read(subjects.toArray(new Subject[subjects.size()]));
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @DELETE
    @Timed
    @Path("admin/remove/object")
    public void removeObjectMapping(Assocation<Object> association) {
        try {
            oracle.remove(association.a, association.b).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @DELETE
    @Timed
    @Path("admin/remove/relation")
    public void removeRelationMapping(Assocation<Relation> association) {
        try {
            oracle.remove(association.a, association.b).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @DELETE
    @Timed
    @Path("admin/remove/subject")
    public void removeSubjectMapping(Assocation<Subject> association) {
        try {
            oracle.remove(association.a, association.b).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        } catch (ExecutionException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.BAD_REQUEST);
        } catch (TimeoutException e) {
            throw new WebApplicationException(e, Response.Status.REQUEST_TIMEOUT);
        }
    }

    @POST
    @Timed
    @Path("admin/subjects")
    public Stream<Subject> subjects(PredicateObject predicateObject) {
        try {
            return oracle.subjects(predicateObject.predicate, predicateObject.object);
        } catch (SQLException e) {
            throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
