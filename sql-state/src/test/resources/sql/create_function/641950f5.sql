-- file:transactions.sql ln:393 expect:true
CREATE FUNCTION invert(x float8) RETURNS float8 LANGUAGE plpgsql AS
$$ begin return 1/x
