-- file:create_aggregate.sql ln:184 expect:true
CREATE FUNCTION float8mi_n(float8, float8) RETURNS float8 AS
$$ SELECT $1 - $2
