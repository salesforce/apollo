-- file:rowtypes.sql ln:188 expect:true
CREATE FUNCTION price_key_from_input(price_input) RETURNS price_key AS $$
    SELECT $1.id
$$ LANGUAGE SQL
