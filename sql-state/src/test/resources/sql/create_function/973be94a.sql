-- file:window.sql ln:323 expect:true
CREATE FUNCTION logging_minvfunc_nonstrict(text, anyelement) RETURNS text AS
$$ SELECT $1 || '-' || quote_nullable($2) $$
LANGUAGE SQL IMMUTABLE
