-- file:truncate.sql ln:185 expect:true
CREATE TABLE truncate_a (id serial,
                         id1 integer default nextval('truncate_a_id1'))
