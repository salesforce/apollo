-- file:typed_table.sql ln:60 expect:true
CREATE FUNCTION namelen(person_type) RETURNS int LANGUAGE SQL AS $$ SELECT length($1.name) $$
