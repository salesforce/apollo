-- file:create_type.sql ln:47 expect:true
CREATE FUNCTION int42_in(cstring)
   RETURNS int42
   AS 'int4in'
   LANGUAGE internal STRICT IMMUTABLE
